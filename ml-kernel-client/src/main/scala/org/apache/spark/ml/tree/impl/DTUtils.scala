// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.tree.impl

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.tree.LearningNode
import org.apache.spark.ml.tree.impl.RandomForest.NodeIndexInfo
import org.apache.spark.mllib.tree.configuration.{Strategy => OldStrategy}
import org.apache.spark.rdd.RDD


final case class DFExtraParams(
    rfParams: RFExtraParams,
    useDFCollPtner: Boolean,
    numFeaturesOptFindSplits: Int,
    var localTrainingThreshold: Option[Int] = None)

final case class DTGroupInfo (
)

object DTUtils extends Logging {

  def parseExtraParams(input: RDD[_], strategy: OldStrategy): DFExtraParams = {
    null
  }

  def getGroupInfo(numNodes: Int,
      treeToNodeToIndexInfo: Map[Int, Map[Int, NodeIndexInfo]],
      extraParams: DFExtraParams,
      nodesForGroup: Map[Int, Array[LearningNode]]): DTGroupInfo = {
    null
  }

  def transformBaggedRDD(baggedInputOri: RDD[BaggedPoint[TreePointY]],
      extraParams: DFExtraParams): RDD[BaggedPoint[TreePointY]] = {
    null
  }


  def initNodeStatsAgg(
      numNodes: Int,
      nodeToFeaturesBc: Broadcast[Option[Map[Int, Array[Int]]]],
      metadata: DecisionTreeMetadata,
      points: Iterator[BaggedPoint[TreePointY]],
      groupInfo: DTGroupInfo): (Option[BaggedPoint[TreePointY]], Array[DTStatsAggregator]) = {
    null
  }

  def isSubSampled(
      baggedPoint: BaggedPoint[TreePointY],
      groupInfo: DTGroupInfo, treeIndex: Int, sampleId: Short): Boolean = {
    true
  }

  def isValidNodeInfo(nodeInfo: NodeIndexInfo, agg: Array[DTStatsAggregator],
      groupInfo: DTGroupInfo,
      baggedPoint: BaggedPoint[TreePointY],
      sampleId: Short): Boolean = {
    true
  }

  def maxNumParallelThreads(): Int = {
    0
  }

  def getInvisibleParamsForMLLib(input: RDD[_]): (Int, Int) = {
    null
  }

}

class DFCollectionPartitioner(
    numPtsOfInput: Int,
    maxNumParallelThreads: Int,
    numFeatures: Int) extends Partitioner {

  def numPartitions: Int = numPtsOfInput

  def getPartition(key: Any): Int = 0

  override def equals(other: Any): Boolean = true

  override def hashCode: Int = numPartitions
}
