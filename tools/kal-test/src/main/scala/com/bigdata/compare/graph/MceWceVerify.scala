package com.bigdata.compare.graph

import org.apache.spark.{SparkConf, SparkContext}

object MceWceVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0) + "/clique_info"
    val path1 = args(1) + "/clique_info"
    val split = ","
    val sparkConf = new SparkConf().setAppName("MCE/WCEVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val rdd0 = sc.textFile(path0).map(s => {
      val arr = s.split(split)
      arr(1)
    }).cache()
    val rdd1 = sc.textFile(path1).map(s => {
      val arr = s.split(split)
      arr(1)
    }).cache()

    val cnt0 = rdd0.count()
    val cnt1 = rdd1.count()
    val flag = cnt0 == cnt1
    sc.stop()
    println(s"Static Nodes Count: $cnt0, $cnt1")
    println(s"The algorithm is correct: ${flag}")
  }
}
