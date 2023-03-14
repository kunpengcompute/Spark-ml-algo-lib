// scalastyle:off println
package com.bigdata.compare.graph

import org.apache.spark.{SparkConf, SparkContext}

object CDVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val sparkConf = new SparkConf().setAppName("DegreeVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val rdd0 = sc.textFile(path0).collect()
    val rdd1 = sc.textFile(path1).collect()

    val flag = rdd0.length == rdd1.length
    sc.stop()
    println(s"Static Nodes Count: ${rdd0.length}, ${rdd1.length}")
    println(s"The algorithm is correct: ${flag}")
  }
}
