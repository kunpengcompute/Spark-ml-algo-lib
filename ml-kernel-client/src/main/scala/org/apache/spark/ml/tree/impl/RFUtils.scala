// scalastyle:off header.matches
/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.tree.impl

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.tree.impl.RandomForest.NodeIndexInfo
import org.apache.spark.mllib.tree.configuration.{Strategy => OldStrategy}
import org.apache.spark.rdd.RDD

object RFExtraParams {
}

final case class RFExtraParams (
                               bcVariables: Boolean,
                               featuresDataType: String)

final case class GroupInfo (
                           )

object RFUtils extends Logging {

  def parseExtraParams(input: RDD[_], strategy: OldStrategy): RFExtraParams = {
    null
  }

  def getGroupInfo(numNodes: Int,
                  nodeIdxInfo: Map[Int, Map[Int, NodeIndexInfo]],
                  extraParams: Option[RFExtraParams]): GroupInfo = {
    null
  }

  def transformBaggedRDD(baggedInputOri: RDD[BaggedPoint[TreePointX]],
                        extraParams: RFExtraParams): RDD[BaggedPoint[TreePointX]] = {
    null
  }

  def initNodeStatsAgg(numNodes: Int,
                      nodeToFeaturesBc: Broadcast[Option[Map[Int, Array[Int]]]],
                      metadata: DecisionTreeMetadata,
                      points: Iterator[BaggedPoint[TreePointX]],
                      groupInfo: GroupInfo):
                        (Option[BaggedPoint[TreePointX]], Array[DTStatsAggregator]) = {
    null
  }

  def isSubSampled(baggedPoint: BaggedPoint[TreePointX],
                  groupInfo: GroupInfo, treeIndex: Int, id: Short): Boolean = {
    true
  }

  def isValidNodeInfo(nodeInfo: NodeIndexInfo, agg: Array[DTStatsAggregator]): Boolean = {
    true
  }

  def isValidAgg(agg: DTStatsAggregator): Boolean = {
    true
  }

}
