package com.bigdata.compare.graph

import org.apache.spark.{SparkConf, SparkContext}

object BFSVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val split = ";"
    val sparkConf = new SparkConf().setAppName("BFSVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val rdd0 = sc.textFile(path0).map(f => {
      val arr = f.split(split)
      (arr(0).toLong, arr(1).split(",").sorted, arr(2).toInt)
    }).map(f => (f._1, f._2.mkString({","}) + f._3))
    val rdd1 = sc.textFile(path1).map(f => {
      val arr = f.split(split)
      (arr(0).toLong, arr(1).split(",").sorted, arr(2).toInt)
    }).map(f => (f._1, f._2.mkString({","}) + f._3))

    val cnt0 = rdd0.count()
    val cnt1 = rdd1.count()
    val statisticR = rdd0.join(rdd1).mapValues(f => f._1 == f._2)
    val joinCnt = statisticR.count()
    val flag = cnt0 == cnt1 && cnt0 == joinCnt
    sc.stop()
    println(s"Static Nodes Count: $joinCnt, $cnt0, $cnt1")
    println(s"The algorithm is correct: $flag")
  }
}
