package com.bigdata.utils

import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.tree.configuration.FeatureType
import org.apache.spark.mllib.tree.model.Node

object DTBucketUtils extends Serializable {
  def getLeafNodes(node: Node): Array[Int] = {
    var treeLeafNodes = new Array[Int](0)
    if (node.isLeaf) {
      treeLeafNodes = treeLeafNodes.:+(node.id)
    } else {
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.leftNode.get)
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.rightNode.get)
    }
    treeLeafNodes
  }

  def predictModify(node: Node, features: SparseVector): Int = {
    val split = node.split
    if (node.isLeaf) {
      node.id
    } else if (split.get.featureType == FeatureType.Continuous) {
      if (features(split.get.feature) <= split.get.threshold) {
        predictModify(node.leftNode.get, features)
      } else {
        predictModify(node.rightNode.get, features)
      }
    }  else if (split.get.categories.contains(features(split.get.feature))) {
      predictModify(node.leftNode.get, features)
    } else {
      predictModify(node.rightNode.get, features)
    }
  }
}
