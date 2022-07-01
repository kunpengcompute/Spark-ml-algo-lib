package org.apache.spark.mllib.tree

import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => MllibDecisionTreeModel}

package object helper {
  type NodeData = org.apache.spark.mllib.tree.model.DecisionTreeModel.SaveLoadV1_0.NodeData
  val NodeData = org.apache.spark.mllib.tree.model.DecisionTreeModel.SaveLoadV1_0.NodeData
  type SplitData = org.apache.spark.mllib.tree.model.DecisionTreeModel.SaveLoadV1_0.SplitData
  val SplitData = org.apache.spark.mllib.tree.model.DecisionTreeModel.SaveLoadV1_0.SplitData
  type PredictData = org.apache.spark.mllib.tree.model.DecisionTreeModel.SaveLoadV1_0.PredictData
  val PredictData = org.apache.spark.mllib.tree.model.DecisionTreeModel.SaveLoadV1_0.PredictData

  def toOldDTModel(dtModel: DecisionTreeClassificationModel): MllibDecisionTreeModel = {
    dtModel.toOld
  }
}
