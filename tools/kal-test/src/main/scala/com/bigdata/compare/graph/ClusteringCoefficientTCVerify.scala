package com.bigdata.compare.graph

import com.bigdata.graph.Util

import org.apache.spark.{SparkConf, SparkContext}

object ClusteringCoefficientTCVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val split = ","
    val partNum = 1000
    val sparkConf = new SparkConf().setAppName("ClusteringCoefficientTCVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val rdd0 = Util.readEdgeFileFromHDFS(sc, path0, split, partNum)
    val rdd1 = Util.readEdgeFileFromHDFS(sc, path1, split, partNum)
    val res = rdd1.union(rdd0).reduceByKey((a, b) => {
      Math.abs(a - b)
    }).filter(f => f._2 > 1e-7)

    val cnt0 = rdd0.count()
    val cnt1 = rdd1.count()
    val flag = res.count() == 0
    sc.stop()
    println(s"Static Nodes Count: $cnt0, $cnt1")
    println(s"The algorithm is correct: ${flag}")
  }
}
