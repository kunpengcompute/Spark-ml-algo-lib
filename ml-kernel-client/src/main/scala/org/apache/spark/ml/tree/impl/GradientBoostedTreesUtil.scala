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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.objects.ObjectArrayList

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.tree.LearningNode
import org.apache.spark.ml.tree.Split
import org.apache.spark.ml.tree.impl.GradientBoostedTreesCore.NodeIndexInfo
import org.apache.spark.mllib.tree.configuration.{Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.ImpurityStats
import org.apache.spark.rdd.RDD

object GradientBoostedTreesUtil extends Logging {

  def dataProcessX(
      input: RDD[LabeledPoint],
      splits: Array[Array[Split]],
      treeStrategy: OldStrategy,
      metadata: DecisionTreeMetadata,
      timer: TimeTracker,
      seed: Long): (RDD[TreePoint], RDD[(Int, (IntArrayList, ObjectArrayList[Split]))],
    Broadcast[DoubleArrayList], Broadcast[Int2ObjectOpenHashMap[IntArrayList]]) = {
    null
  }

  def nodeIdCacheXConstruction(
      nodes: Array[LearningNode],
      rawPartInfoBc: Broadcast[Int2ObjectOpenHashMap[IntArrayList]])
    : Int2ObjectOpenHashMap[Int2ObjectOpenHashMap[IntArrayList]] = {
    null
  }

  def chooseBestSplits(
      input: RDD[(Int, (IntArrayList, ObjectArrayList[Split]))],
      nodeIndexInfo: Map[Int, Map[Int, NodeIndexInfo]],
      metadata: DecisionTreeMetadata,
      nodeIdCacheBc: Broadcast[Int2ObjectOpenHashMap[Int2ObjectOpenHashMap[IntArrayList]]],
      labelArrayBc: Broadcast[DoubleArrayList],
      nodes: Array[LearningNode]): scala.collection.Map[Int, (Split, ImpurityStats)] = {
    null
  }




  def updateNodeIdCache(
      nodeIdCache: Int2ObjectOpenHashMap[Int2ObjectOpenHashMap[IntArrayList]],
      nodeIdCacheBc: Broadcast[Int2ObjectOpenHashMap[Int2ObjectOpenHashMap[IntArrayList]]],
      input: RDD[TreePoint],
      nodesForGroup: Map[Int, Array[LearningNode]],
      nodeIndexInfo: Map[Int, Map[Int, NodeIndexInfo]],
      splits: Array[Array[Split]],
      rawPartInfoBc: Broadcast[Int2ObjectOpenHashMap[IntArrayList]],
      metadata: DecisionTreeMetadata,
      timer: TimeTracker): Unit = {
  }

}
