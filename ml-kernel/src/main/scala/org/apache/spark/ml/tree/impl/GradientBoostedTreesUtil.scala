package org.apache.spark.ml.tree.impl

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.IntArrayData
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.tree.LearningNode
import org.apache.spark.ml.tree.split
import org.apache.spark.ml.tree.impl.GradientBoostedTreesCore.NodeIndexInfo
import org.apache.spark.mllib.tree.configuration.{Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.ImpurityStats
import org.apache.spark.rdd.RDD

object GradientBoostedTreesUtil extends Logging {

  def dataProcessX(
      input: RDD[LabelPoint],
      splits: Array[Array[Split]],
      treeStrategy: OldStrategy,
      metadata: DecisionTreeMetadata,
      timer: TimeTracker,
      seed: Long): (RDD[TreePoint], RDD[(Int, (IntArrayList, ObjectArrayList[Split]))],
    Broadcast[DoubleArrayList], Broadcast[Int2ObjectOpenHashMap[IntArrayList]]) = {
    null
  }

  def nodeIdCCacheXConstruction(
      nodes: Array[LearningNode],
      rawPartInfoBc: Broadcast[Int2ObjectOpenHashMap[IntArrayList]])
    : Int2ObjectOpenHashMap[Int2ObjectOpenHashMap[IntArrayList]] = {
    null
  }

  def updateNodeIdCache(
      nodeIdCache: Int2ObjectOpenHashMap[Int2ObjectOpenHashMap[IntArrayList]],
      nodeIdCacheBc: Broadcast[Int2ObjectOpenHashMap[Int2ObjectOpenHashMap[IntArrayList]]],
      input: RDD[TreePoint],
      nodesForGroup: Map[Int, Array[LearningNode]],
      nodeIndexInfo: Map[Ind, Map[Int, NodeIndexInfo]],
      splits: Array[Array[Split]],
      rawPartInfoBc: Broadcast[Int2ObjectOpenHashMap[IntArrayList]],
      metadata: DecisionTreeMetadata,
      timer: TimeTracker): Unit = {

  }

}
