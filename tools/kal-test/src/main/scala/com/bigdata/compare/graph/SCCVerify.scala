package com.bigdata.compare.graph

import org.apache.spark.{SparkConf, SparkContext}

object SCCVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val split = ","
    val sparkConf = new SparkConf().setAppName("SCCVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val rdd0 = sc.textFile(path0).map(s => {
      val arr = s.split(split)
      (arr(0).toLong, arr(1).toLong)
    }).cache()
    val rdd1 = sc.textFile(path1).map(s => {
      val arr = s.split(split)
      (arr(0).toLong, arr(1).toLong)
    }).cache()
    val rdd00 = rdd0.groupBy(_._2).flatMap(f => {
      val arr = f._2.toArray
      val minId = arr.map(_._1).min
      arr.map(f => (f._1, minId))
    }).cache()
    val rdd11 = rdd1.groupBy(_._2).flatMap(f => {
      val arr = f._2.toArray
      val minId = arr.map(_._1).min
      arr.map(f => (f._1, minId))
    }).cache()

    val cnt0 = rdd0.count()
    val cnt1 = rdd1.count()
    val statisticR = rdd00.join(rdd11).filter {
      case (vid, (sc1, sc2)) => sc1 == sc2
    }
    val joinCnt = statisticR.count()
    val flag = cnt0 == cnt1 && cnt0 == joinCnt
    sc.stop()
    println(s"Static Nodes Count: $joinCnt, $cnt0, $cnt1")
    println(s"The algorithm is correct: ${flag}")
  }
}
