package com.bigdata.compare.graph

import com.bigdata.graph.Util

import org.apache.spark.{SparkConf, SparkContext}

object BetweennessClosenessVerify {
  def main(args: Array[String]): Double = {
    val groundTruthPath = args(0)
    val output = args(1)
    val numPartitions = 232
    val sparkConf = new SparkConf().setAppName("BetweennessClosenessVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val groundTruthSet = Util
      .readTopKResultFromHDFS(sc, groundTruthPath, ",", numPartitions)
      .collect()
      .toSet
    val resultSet = Util
      .readTopKResultFromHDFS(sc, output, ",", numPartitions)
      .collect()
      .toSet
    val accuracy = groundTruthSet
      .intersect(resultSet)
      .size
      .toDouble / groundTruthSet.size
    val flag = accuracy >= 0.93

    sc.stop()
    println(s"Accuracy: ${accuracy}")
    println(s"The algorithm is correct: ${flag}")
    return accuracy
  }
}
