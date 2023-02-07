package com.bigdata.compare.graph

import com.bigdata.graph.Util

import org.apache.spark.graphx.lib.Modularity
import org.apache.spark.{SparkConf, SparkContext}

object LpaVerify {
  def main(args: Array[String]): Unit = {
    val dataset = args(0)
    val path0 = args(1)
    val path1 = args(2)
    val inputSplit = " "
    val outputSplit = ","
    val partNum = 232
    val sparkConf = new SparkConf().setAppName("LpaVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val nodes0 = Util.readCommFromHDFS(sc, path0, outputSplit, partNum)
    val nodes1 = Util.readCommFromHDFS(sc, path1, outputSplit, partNum)
    val edges = Util.readGraphFromHDFS(sc, dataset, inputSplit, false, partNum)
    val modularity0 = Modularity.run(nodes0, edges, false, partNum)
    val modularity1 = Modularity.run(nodes1, edges, false, partNum)
    val m0 = modularity0.formatted("%.5f")
    val m1 = modularity1.formatted("%.5f")
    val flag = m0 <= m1
    sc.stop()
    println(s"Modularity: ${modularity0}, ${modularity1}.")
    println(s"The algorithm is correct: ${flag}")
  }
}
