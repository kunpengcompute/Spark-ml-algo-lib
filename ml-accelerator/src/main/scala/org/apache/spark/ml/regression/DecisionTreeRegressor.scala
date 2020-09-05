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

package org.apache.spark.ml.regression

import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, IntArrayList}
import it.unimi.dsi.fastutil.objects.ObjectArrayList

import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Predictor
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.DecisionTreeMetadata
import org.apache.spark.ml.tree.impl.RandomForest4GBDTX
import org.apache.spark.ml.tree.impl.RandomForestRaw
import org.apache.spark.ml.tree.impl.TreePoint
import org.apache.spark.ml.util._
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => OldDecisionTreeModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset


/**
 * <a href="http://en.wikipedia.org/wiki/Decision_tree_learning">Decision tree</a>
 * learning algorithm for regression.
 * It supports both continuous and categorical features.
 */
@Since("1.4.0")
class DecisionTreeRegressor @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Predictor[Vector, DecisionTreeRegressor, DecisionTreeRegressionModel]
  with DecisionTreeRegressorParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("dtr"))

  // Override parameter setters from parent trait for Java API compatibility.
  /** @group setParam */
  @Since("1.4.0")
  override def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * Must be at least 1.
   * (default = 10)
   * @group setParam
   */
  @Since("1.4.0")
  override def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setImpurity(value: String): this.type = set(impurity, value)

  /** @group setParam */
  @Since("1.6.0")
  override def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("2.0.0")
  def setVarianceCol(value: String): this.type = set(varianceCol, value)

  override protected def train(dataset: Dataset[_]): DecisionTreeRegressionModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val strategy = getOldStrategy(categoricalFeatures)

    val instr = Instrumentation.create(this, oldDataset)
    instr.logParams(params: _*)

    val trees = RandomForestRaw.run(oldDataset, strategy, numTrees = 1, featureSubsetStrategy = "all",
      seed = $(seed), instr = Some(instr), parentUID = Some(uid))

    val m = trees.head.asInstanceOf[DecisionTreeRegressionModel]
    instr.logSuccess(m)
    m
  }

  /** (private[ml]) Train a decision tree on an RDD */
  private[ml] def train(
      data: RDD[LabeledPoint],
      oldStrategy: OldStrategy,
      featureSubsetStrategy: String): DecisionTreeRegressionModel = {
    val instr = Instrumentation.create(this, data)
    instr.logParams(params: _*)

    val trees = RandomForestRaw.run(data, oldStrategy, numTrees = 1, featureSubsetStrategy,
      seed = $(seed), instr = Some(instr), parentUID = Some(uid))

    val m = trees.head.asInstanceOf[DecisionTreeRegressionModel]
    instr.logSuccess(m)
    m
  }

  /** (private[ml]) Train a decision tree on an RDD */
  private[ml] def train4GBDTX(
    labelArrayBc: Broadcast[DoubleArrayList],
    processedInput: RDD[(Int, (IntArrayList, ObjectArrayList[Split]))],
    metadata: DecisionTreeMetadata,
    splits: Array[Array[Split]],
    oldStrategy: OldStrategy,
    featureSubsetStrategy: String,
    input: RDD[TreePoint],
    rawPartInfoBc: Broadcast[Int2ObjectOpenHashMap[IntArrayList]]):
    DecisionTreeRegressionModel = {
      var instr = Instrumentation.create(this, processedInput)
      instr.logParams(params: _*)

      var trees = RandomForest4GBDTX.runX(labelArrayBc, processedInput, metadata,
        splits, oldStrategy, numTrees = 1, seed = $(seed), input,
        rawPartInfoBc, parentUID = Some(uid))

      val m = trees.head.asInstanceOf[DecisionTreeRegressionModel]
      instr.logSuccess(m)
      m
    }


  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(categoricalFeatures: Map[Int, Int]): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses = 0, OldAlgo.Regression, getOldImpurity,
      subsamplingRate = 1.0)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): DecisionTreeRegressor = defaultCopy(extra)
}

@Since("1.4.0")
object DecisionTreeRegressor extends DefaultParamsReadable[DecisionTreeRegressor] {
  /** Accessor for supported impurities: variance */
  final val supportedImpurities: Array[String] = TreeRegressorParams.supportedImpurities

  @Since("2.0.0")
  override def load(path: String): DecisionTreeRegressor = super.load(path)
}

