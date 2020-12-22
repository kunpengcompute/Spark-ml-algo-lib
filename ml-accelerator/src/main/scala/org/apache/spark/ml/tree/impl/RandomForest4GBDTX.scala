/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.tree.impl

import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, IntArrayList}
import it.unimi.dsi.fastutil.objects.ObjectArrayList
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.GradientBoostedTreesCore.NodeIndexInfo
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.ImpurityStats
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.{SamplingUtils, XORShiftRandom}


/**
 * ALGORITHM
 *
 * This is a sketch of the algorithm to help new developers.
 *
 * The algorithm partitions data by instances (rows).
 * On each iteration, the algorithm splits a set of nodes.  In order to choose the best split
 * for a given node, sufficient statistics are collected from the distributed data.
 * For each node, the statistics are collected to some worker node, and that worker selects
 * the best split.
 *
 * This setup requires discretization of continuous features.  This binning is done in the
 * findSplits() method during initialization, after which each continuous feature becomes
 * an ordered discretized feature with at most maxBins possible values.
 *
 * The main loop in the algorithm operates on a queue of nodes (nodeStack).  These nodes
 * lie at the periphery of the tree being trained.  If multiple trees are being trained at once,
 * then this queue contains nodes from all of them.  Each iteration works roughly as follows:
 *   On the master node:
 *     - Some number of nodes are pulled off of the queue (based on the amount of memory
 *       required for their sufficient statistics).
 *     - For random forests, if featureSubsetStrategy is not "all," then a subset of candidate
 *       features are chosen for each node.  See method selectNodesToSplit().
 *   On worker nodes, via method findBestSplits():
 *     - The worker makes one pass over its subset of instances.
 *     - For each (tree, node, feature, split) tuple, the worker collects statistics about
 *       splitting.  Note that the set of (tree, node) pairs is limited to the nodes selected
 *       from the queue for this iteration.  The set of features considered can also be limited
 *       based on featureSubsetStrategy.
 *     - For each node, the statistics for that node are aggregated to a particular worker
 *       via reduceByKey().  The designated worker chooses the best (feature, split) pair,
 *       or chooses to stop splitting if the stopping criteria are met.
 *   On the master node:
 *     - The master collects all decisions about splitting nodes and updates the model.
 *     - The updated model is passed to the workers on the next iteration.
 * This process continues until the node queue is empty.
 *
 * Most of the methods in this implementation support the statistics aggregation, which is
 * the heaviest part of the computation.  In general, this implementation is bound by either
 * the cost of statistics computation on workers or by communicating the sufficient statistics.
 */
private[spark] object RandomForest4GBDTX extends Logging {

  /**
   * Train a random forest.
   *
   * @param input Training data: RDD of `LabeledPoint`
   * @return an unweighted set of trees
   */
  def runX(
      labelArrayBc: Broadcast[DoubleArrayList],
      processedInput: RDD[(Int, (IntArrayList, ObjectArrayList[Split]))],
      metadata: DecisionTreeMetadata,
      splits: Array[Array[Split]],
      strategy: OldStrategy,
      numTrees: Int,
      seed: Long,
      input: RDD[TreePoint],
      rawPartInfoBc: Broadcast[Int2ObjectOpenHashMap[IntArrayList]],
      parentUID: Option[String] = None): Array[DecisionTreeModel] = {

    val timer = new TimeTracker()

    timer.start("total")

    timer.start("init")

    // depth of the decision tree
    val maxDepth = strategy.maxDepth
    require(maxDepth <= 30,
      s"DecisionTree currently only supports maxDepth <= 30, but was given maxDepth = $maxDepth.")

    // Max memory usage for aggregates
    // TODO: Calculate memory usage more precisely.
    val maxMemoryUsage: Long = strategy.maxMemoryInMB * 1024L * 1024L
    logDebug(s"max memory usage for aggregates = ${maxMemoryUsage} bytes.")

    /*
      Stack of nodes to train: (treeIndex, node)
      The reason this is a stack is that we train many trees at once, but we want to focus on
      completing trees, rather than training all simultaneously.  If we are splitting nodes from
      1 tree, then the new nodes to split will be put at the top of this stack, so we will continue
      training the same tree in the next iteration.  This focus allows us to send fewer trees to
      workers on each iteration; see topNodesForGroup below.
     */
    val nodeStack = new mutable.ArrayStack[(Int, LearningNode)]

    val rng = new Random()
    rng.setSeed(seed)

    // Allocate and queue root nodes.
    val topNodes = Array.fill[LearningNode](numTrees)(LearningNode.emptyNode(nodeIndex = 1))
    Range(0, numTrees).foreach(treeIndex => nodeStack.push((treeIndex, topNodes(treeIndex))))

    val nodeIdCacheX = GradientBoostedTreesUtil.nodeIdCacheXConstruction(topNodes, rawPartInfoBc)
    timer.stop("init")

    while (nodeStack.nonEmpty) {
      // Collect some nodes to split, and choose features for each node (if subsampling).
      // Each group of nodes may come from one or multiple trees, and at multiple levels.
      val (nodesForGroup, treeToNodeToIndexInfo) =
        RandomForest4GBDTX.selectNodesToSplitX(nodeStack, maxMemoryUsage, metadata, rng)
      // Sanity check (should never occur):
      assert(nodesForGroup.nonEmpty,
        s"RandomForest selected empty nodesForGroup.  Error for unknown reason.")

      // Only send trees to worker if they contain nodes being split this iteration.
      val topNodesForGroup: Map[Int, LearningNode] =
        nodesForGroup.keys.map(treeIdx => treeIdx -> topNodes(treeIdx)).toMap

      // Choose node splits, and enqueue new nodes as needed.
      timer.start("findBestSplits")
      RandomForest4GBDTX.findBestSplitsX(labelArrayBc, processedInput, metadata,
        (nodesForGroup, treeToNodeToIndexInfo), splits, nodeStack, nodeIdCacheX, input,
        rawPartInfoBc, timer)
      timer.stop("findBestSplits")
    }

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")

    val numFeatures = metadata.numFeatures

    parentUID match {
      case Some(uid) =>
        if (strategy.algo == OldAlgo.Classification) {
          // unreachable for GBDT
          topNodes.map { rootNode =>
            new DecisionTreeClassificationModel(uid, rootNode.toNode, numFeatures,
              strategy.getNumClasses)
          }
        } else {
          topNodes.map { rootNode =>
            new DecisionTreeRegressionModel(uid, rootNode.toNode, numFeatures)
          }
        }
      // unreachable for GBDT
      case None =>
        if (strategy.algo == OldAlgo.Classification) {
          topNodes.map { rootNode =>
            new DecisionTreeClassificationModel(rootNode.toNode, numFeatures,
              strategy.getNumClasses)
          }
        } else {
          topNodes.map(rootNode => new DecisionTreeRegressionModel(rootNode.toNode, numFeatures))
        }
    }
  }

  /**
   * Given a group of nodes, this finds the best split for each node.
   *
   * @param input Training data: RDD of [[TreePoint]]
   * @param metadata Learning and dataset metadata
   * @param splits possible splits for all features, indexed (numFeatures)(numSplits)
   * @param nodeStack  Queue of nodes to split, with values (treeIndex, node).
   *                   Updated with new non-leaf nodes which are created.
   * @param nodeIdCache Node Id cache containing an RDD of Array[Int] where
   *                    each value in the array is the data point's node Id
   *                    for a corresponding tree. This is used to prevent the need
   *                    to pass the entire tree to the executors during
   *                    the node stat aggregation phase.
   */
  private[tree] def findBestSplitsX(
      labelArrayBc: Broadcast[DoubleArrayList],
      processedInput: RDD[(Int, (IntArrayList, ObjectArrayList[Split]))],
      metadata: DecisionTreeMetadata,
      packagedNodeInfo: (Map[Int, Array[LearningNode]], Map[Int, Map[Int, NodeIndexInfo]]),
      splits: Array[Array[Split]],
      nodeStack: mutable.ArrayStack[(Int, LearningNode)],
      nodeIdCache: Int2ObjectOpenHashMap[Int2ObjectOpenHashMap[IntArrayList]],
      input: RDD[TreePoint],
      rawPartInfoBc: Broadcast[Int2ObjectOpenHashMap[IntArrayList]],
      timer: TimeTracker = new TimeTracker) : Unit = {

    /*
     * The high-level descriptions of the best split optimizations are noted here.
     *
     * *Group-wise training*
     * We perform bin calculations for groups of nodes to reduce the number of
     * passes over the data.  Each iteration requires more computation and storage,
     * but saves several iterations over the data.
     *
     * *Bin-wise computation*
     * We use a bin-wise best split computation strategy instead of a straightforward best split
     * computation strategy. Instead of analyzing each sample for contribution to the left/right
     * child node impurity of every split, we first categorize each feature of a sample into a
     * bin. We exploit this structure to calculate aggregates for bins and then use these aggregates
     * to calculate information gain for each split.
     *
     * *Aggregation over partitions*
     * Instead of performing a flatMap/reduceByKey operation, we exploit the fact that we know
     * the number of splits in advance. Thus, we store the aggregates (at the appropriate
     * indices) in a single array for all bins and rely upon the RDD aggregate method to
     * drastically reduce the communication overhead.
     */

    // Un-package node info
    val (nodesForGroup, treeToNodeToIndexInfo) = packagedNodeInfo
    // numNodes:  Number of nodes in this group
    val numNodes = nodesForGroup.values.map(_.length).sum
    logDebug(s"numNodes = ${numNodes}")
    logDebug(s"numFeatures = ${metadata.numFeatures}")
    logDebug(s"numClasses = ${metadata.numClasses}")
    logDebug(s"isMulticlass = ${metadata.isMulticlass}")
    logDebug(s"isMulticlassWithCategoricalFeatures =" +
      s"${metadata.isMulticlassWithCategoricalFeatures}")

    // array of nodes to train indexed by node index in group
    val nodes = new Array[LearningNode](numNodes)
    nodesForGroup.foreach { case (treeIndex, nodesForTree) =>
      nodesForTree.foreach { node =>
        nodes(treeToNodeToIndexInfo(treeIndex)(node.id).nodeIndexInGroup) = node
      }
    }

    timer.start("broadcast")
    val nodeIdCacheBc = processedInput.sparkContext.broadcast(nodeIdCache)
    timer.stop("broadcast")

    // Calculate best splits for all nodes in the group
    timer.start("chooseSplits")

    val nodeToBestSplits = GradientBoostedTreesUtil.chooseBestSplits(processedInput,
      treeToNodeToIndexInfo, metadata, nodeIdCacheBc, labelArrayBc, nodes)

    timer.stop("chooseSplits")

    // Iterate over all nodes in this group.
    nodesForGroup.foreach { case (treeIndex, nodesForTree) =>
      nodesForTree.foreach { node =>
        val nodeIndex = node.id
        val nodeInfo = treeToNodeToIndexInfo(treeIndex)(nodeIndex)
        val aggNodeIndex = nodeInfo.nodeIndexInGroup
        val (split: Split, stats: ImpurityStats) =
          nodeToBestSplits(nodeIndex)
        logDebug(s"best split = ${split}")

        // Extract info for this node.  Create children if not leaf.
        val isLeaf =
          (stats.gain <= 0) || (LearningNode.indexToLevel(nodeIndex) == metadata.maxDepth)
        node.isLeaf = isLeaf
        node.stats = stats
        logDebug(s"Node = ${node}")

        if (!isLeaf) {
          node.split = Some(split)
          val childIsLeaf = (LearningNode.indexToLevel(nodeIndex) + 1) == metadata.maxDepth
          val leftChildIsLeaf = childIsLeaf || (stats.leftImpurity == 0.0)
          val rightChildIsLeaf = childIsLeaf || (stats.rightImpurity == 0.0)
          node.leftChild = Some(LearningNode(LearningNode.leftChildIndex(nodeIndex),
            leftChildIsLeaf, ImpurityStats.getEmptyImpurityStats(stats.leftImpurityCalculator)))
          node.rightChild = Some(LearningNode(LearningNode.rightChildIndex(nodeIndex),
            rightChildIsLeaf, ImpurityStats.getEmptyImpurityStats(stats.rightImpurityCalculator)))

          // enqueue left child and right child if they are not leaves
          if (!leftChildIsLeaf) {
            nodeStack.push((treeIndex, node.leftChild.get))
          }
          if (!rightChildIsLeaf) {
            nodeStack.push((treeIndex, node.rightChild.get))
          }

          logDebug(s"leftChildIndex = ${node.leftChild.get.id}" +
            s", impurity = ${stats.leftImpurity}")
          logDebug(s"rightChildIndex = ${node.rightChild.get.id}" +
            s", impurity = ${stats.rightImpurity}")
        }
      }
    }

    GradientBoostedTreesUtil.updateNodeIdCache(nodeIdCache, nodeIdCacheBc, input, nodesForGroup,
      treeToNodeToIndexInfo, splits, rawPartInfoBc, metadata, timer)
  }

  /**
   * Returns splits for decision tree calculation.
   * Continuous and categorical features are handled differently.
   *
   * Continuous features:
   *   For each feature, there are numBins - 1 possible splits representing the possible binary
   *   decisions at each node in the tree.
   *   This finds locations (feature values) for splits using a subsample of the data.
   *
   * Categorical features:
   *   For each feature, there is 1 bin per split.
   *   Splits and bins are handled in 2 ways:
   *   (a) "unordered features"
   *       For multiclass classification with a low-arity feature
   *       (i.e., if isMulticlass && isSpaceSufficientForAllCategoricalSplits),
   *       the feature is split based on subsets of categories.
   *   (b) "ordered features"
   *       For regression and binary classification,
   *       and for multiclass classification with a high-arity feature,
   *       there is one bin per category.
   *
   * @param input Training data: RDD of [[LabeledPoint]]
   * @param metadata Learning and dataset metadata
   * @param seed random seed
   * @return Splits, an Array of [[Split]]
   *          of size (numFeatures, numSplits)
   */
  protected[tree] def findSplits(
      input: RDD[LabeledPoint],
      metadata: DecisionTreeMetadata,
      seed: Long): Array[Array[Split]] = {

    logDebug(s"isMulticlass = ${metadata.isMulticlass}")

    val numFeatures = metadata.numFeatures

    // Sample the input only if there are continuous features.
    val continuousFeatures = Range(0, numFeatures).filter(metadata.isContinuous)
    val sampledInput = if (continuousFeatures.nonEmpty) {
      // Calculate the number of samples for approximate quantile calculation.
      val requiredSamples = math.max(metadata.maxBins * metadata.maxBins, 10000)
      val fraction = if (requiredSamples < metadata.numExamples) {
        requiredSamples.toDouble / metadata.numExamples
      } else {
        1.0
      }
      logDebug(s"fraction of data used for calculating quantiles = ${fraction}")
      input.sample(withReplacement = false, fraction, new XORShiftRandom(seed).nextInt())
    } else {
      input.sparkContext.emptyRDD[LabeledPoint]
    }

    findSplitsBySorting(sampledInput, metadata, continuousFeatures)
  }

  private def findSplitsBySorting(
      input: RDD[LabeledPoint],
      metadata: DecisionTreeMetadata,
      continuousFeatures: IndexedSeq[Int]): Array[Array[Split]] = {

    val continuousSplits: scala.collection.Map[Int, Array[Split]] = {
      // reduce the parallelism for split computations when there are less
      // continuous features than input partitions. this prevents tasks from
      // being spun up that will definitely do no work.
      val numPartitions = math.min(continuousFeatures.length, input.partitions.length)

      input
        .flatMap(point => continuousFeatures.map(idx => (idx, point.features(idx))))
        .groupByKey(numPartitions)
        .map { case (idx, samples) =>
          val thresholds = findSplitsForContinuousFeature(samples, metadata, idx)
          val splits: Array[Split] = thresholds.map(thresh => new ContinuousSplit(idx, thresh))
          logDebug(s"featureIndex = $idx, numSplits = ${splits.length}")
          (idx, splits)
        }.collectAsMap()
    }

    val numFeatures = metadata.numFeatures
    val splits: Array[Array[Split]] = Array.tabulate(numFeatures) {
      case i if metadata.isContinuous(i) =>
        val split = continuousSplits(i)
        metadata.setNumSplits(i, split.length)
        split

      // unreachable for GBDT
      case i if metadata.isCategorical(i) && metadata.isUnordered(i) =>
        // Unordered features
        // 2^(maxFeatureValue - 1) - 1 combinations
        val featureArity = metadata.featureArity(i)
        Array.tabulate[Split](metadata.numSplits(i)) { splitIndex =>
          val categories = extractMultiClassCategories(splitIndex + 1, featureArity)
          new CategoricalSplit(i, categories.toArray, featureArity)
        }

      case i if metadata.isCategorical(i) =>
        // Ordered features
        //   Splits are constructed as needed during training.
        Array.empty[Split]
    }
    splits
  }

  /**
   * Nested method to extract list of eligible categories given an index. It extracts the
   * position of ones in a binary representation of the input. If binary
   * representation of an number is 01101 (13), the output list should (3.0, 2.0,
   * 0.0). The maxFeatureValue depict the number of rightmost digits that will be tested for ones.
   */
  private[tree] def extractMultiClassCategories(
      input: Int,
      maxFeatureValue: Int): List[Double] = {
    var categories = List[Double]()
    var j = 0
    var bitShiftedInput = input
    while (j < maxFeatureValue) {
      if (bitShiftedInput % 2 != 0) {
        // updating the list of categories.
        categories = j.toDouble :: categories
      }
      // Right shift by one
      bitShiftedInput = bitShiftedInput >> 1
      j += 1
    }
    categories
  }

  /**
   * Find splits for a continuous feature
   * NOTE: Returned number of splits is set based on `featureSamples` and
   *       could be different from the specified `numSplits`.
   *       The `numSplits` attribute in the `DecisionTreeMetadata` class will be set accordingly.
   *
   * @param featureSamples feature values of each sample
   * @param metadata decision tree metadata
   *                 NOTE: `metadata.numbins` will be changed accordingly
   *                       if there are not enough splits to be found
   * @param featureIndex feature index to find splits
   * @return array of split thresholds
   */
  private[tree] def findSplitsForContinuousFeature(
      featureSamples: Iterable[Double],
      metadata: DecisionTreeMetadata,
      featureIndex: Int): Array[Double] = {
    require(metadata.isContinuous(featureIndex),
      "findSplitsForContinuousFeature can only be used to find splits for a continuous feature.")

    val splits: Array[Double] = if (featureSamples.isEmpty) {
      Array.empty[Double]
    } else {
      val numSplits = metadata.numSplits(featureIndex)

      // get count for each distinct value
      val (valueCountMap, numSamples) = featureSamples.foldLeft((Map.empty[Double, Int], 0)) {
        case ((m, cnt), x) =>
          (m + ((x, m.getOrElse(x, 0) + 1)), cnt + 1)
      }
      // sort distinct values
      val valueCounts = valueCountMap.toSeq.sortBy(_._1).toArray

      val possibleSplits = valueCounts.length - 1
      if (possibleSplits == 0) {
        // constant feature
        Array.empty[Double]
      } else if (possibleSplits <= numSplits) {
        // if possible splits is not enough or just enough, just return all possible splits
        (1 to possibleSplits)
          .map(index => (valueCounts(index - 1)._1 + valueCounts(index)._1) / 2.0)
          .toArray
      } else {
        // stride between splits
        val stride: Double = numSamples.toDouble / (numSplits + 1)
        logDebug(s"stride = ${stride}")

        // iterate `valueCount` to find splits
        val splitsBuilder = mutable.ArrayBuilder.make[Double]
        var index = 1
        // currentCount: sum of counts of values that have been visited
        var currentCount = valueCounts(0)._2
        // targetCount: target value for `currentCount`.
        // If `currentCount` is closest value to `targetCount`,
        // then current value is a split threshold.
        // After finding a split threshold, `targetCount` is added by stride.
        var targetCount = stride
        while (index < valueCounts.length) {
          val previousCount = currentCount
          currentCount += valueCounts(index)._2
          val previousGap = math.abs(previousCount - targetCount)
          val currentGap = math.abs(currentCount - targetCount)
          // If adding count of current value to currentCount
          // makes the gap between currentCount and targetCount smaller,
          // previous value is a split threshold.
          if (previousGap < currentGap) {
            splitsBuilder += (valueCounts(index - 1)._1 + valueCounts(index)._1) / 2.0
            targetCount += stride
          }
          index += 1
        }

        splitsBuilder.result()
      }
    }
    splits
  }

  /**
   * Pull nodes off of the queue, and collect a group of nodes to be split on this iteration.
   * This tracks the memory usage for aggregates and stops adding nodes when too much memory
   * will be needed; this allows an adaptive number of nodes since different nodes may require
   * different amounts of memory (if featureSubsetStrategy is not "all").
   *
   * @param nodeStack  Queue of nodes to split.
   * @param maxMemoryUsage  Bound on size of aggregate statistics.
   * @return  (nodesForGroup, treeToNodeToIndexInfo).
   *          nodesForGroup holds the nodes to split: treeIndex --> nodes in tree.
   *
   *          treeToNodeToIndexInfo holds indices selected features for each node:
   *            treeIndex --> (global) node index --> (node index in group, feature indices).
   *          The (global) node index is the index in the tree; the node index in group is the
   *           index in [0, numNodesInGroup) of the node in this group.
   *          The feature indices are None if not subsampling features.
   */
  private[tree] def selectNodesToSplitX(
      nodeStack: mutable.ArrayStack[(Int, LearningNode)],
      maxMemoryUsage: Long,
      metadata: DecisionTreeMetadata,
      rng: Random): (Map[Int, Array[LearningNode]], Map[Int, Map[Int, NodeIndexInfo]]) = {
    // Collect some nodes to split:
    //  nodesForGroup(treeIndex) = nodes to split
    val mutableNodesForGroup = new mutable.HashMap[Int, mutable.ArrayBuffer[LearningNode]]()
    val mutableTreeToNodeToIndexInfo =
      new mutable.HashMap[Int, mutable.HashMap[Int, NodeIndexInfo]]()
    var memUsage: Long = 0L
    var numNodesInGroup = 0
    // If maxMemoryInMB is set very small, we want to still try to split 1 node,
    // so we allow one iteration if memUsage == 0.
    var groupDone = false
    while (nodeStack.nonEmpty && !groupDone) {
      val (treeIndex, node) = nodeStack.top
      // Choose subset of features for node (if subsampling).
      val featureSubset: Option[Array[Int]] = if (metadata.subsamplingFeatures) {
        Some(SamplingUtils.reservoirSampleAndCount(Range(0,
          metadata.numFeatures).iterator, metadata.numFeaturesPerNode, rng.nextLong())._1)
      } else {
        None
      }
      val featureSubsetHashSetX: Option[mutable.HashSet[Int]] = if (metadata.subsamplingFeatures) {
        Some(scala.collection.mutable.HashSet(featureSubset.get: _*))
      } else {
        None
      }
      // Check if enough memory remains to add this node to the group.
      val nodeMemUsage = RandomForest4GBDTX.aggregateSizeForNode(metadata, featureSubset) * 8L
      if (memUsage + nodeMemUsage <= maxMemoryUsage || memUsage == 0) {
        nodeStack.pop()
        mutableNodesForGroup.getOrElseUpdate(treeIndex, new mutable.ArrayBuffer[LearningNode]()) +=
          node
        mutableTreeToNodeToIndexInfo
          .getOrElseUpdate(treeIndex, new mutable.HashMap[Int, NodeIndexInfo]())(node.id)
          = new NodeIndexInfo(numNodesInGroup, featureSubset, featureSubsetHashSetX)
        numNodesInGroup += 1
        memUsage += nodeMemUsage
      } else {
        groupDone = true
      }
    }
    if (memUsage > maxMemoryUsage) {
      // If maxMemoryUsage is 0, we should still allow splitting 1 node.
      logWarning(s"Tree learning is using approximately $memUsage bytes per iteration, which" +
        s" exceeds requested limit maxMemoryUsage=$maxMemoryUsage. This allows splitting" +
        s" $numNodesInGroup nodes in this iteration.")
    }
    // Convert mutable maps to immutable ones.
    val nodesForGroup: Map[Int, Array[LearningNode]] =
      mutableNodesForGroup.mapValues(_.toArray).toMap
    val treeToNodeToIndexInfo = mutableTreeToNodeToIndexInfo.mapValues(_.toMap).toMap
    (nodesForGroup, treeToNodeToIndexInfo)
  }

  /**
   * Get the number of values to be stored for this node in the bin aggregates.
   *
   * @param featureSubset  Indices of features which may be split at this node.
   *                       If None, then use all features.
   */
  private def aggregateSizeForNode(
      metadata: DecisionTreeMetadata,
      featureSubset: Option[Array[Int]]): Long = {
    val totalBins = if (featureSubset.nonEmpty) {
      featureSubset.get.map(featureIndex => metadata.numBins(featureIndex).toLong).sum
    } else {
      metadata.numBins.map(_.toLong).sum
    }
    if (metadata.isClassification) {
      // unreachable for GBDT
      metadata.numClasses * totalBins
    } else {
      3 * totalBins
    }
  }
}
