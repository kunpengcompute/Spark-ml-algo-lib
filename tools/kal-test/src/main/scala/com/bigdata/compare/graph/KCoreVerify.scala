package com.bigdata.compare.graph

import org.apache.spark.{SparkConf, SparkContext}

object KCoreVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val split = ","
    val sparkConf = new SparkConf().setAppName("KCoreVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val rdd0 = sc.textFile(path0).map(s => {
      val arr = s.split(split)
      (arr(0).trim.toLong, arr(1).trim.toInt)
    }).cache()
    val rdd1 = sc.textFile(path1).map(s => {
      val arr = s.split(split)
      (arr(0).trim.toLong, arr(1).trim.toInt)
    }).cache()

    val cnt0 = rdd0.count()
    val cnt1 = rdd1.count()
    val statisticR = rdd0.join(rdd1).mapValues(f => f._1 == f._2)
    val joinCnt = statisticR.count()
    val equalsCnt = statisticR.filter(_._2).count()
    val flag = cnt0 == cnt1 && cnt0 == joinCnt && joinCnt == equalsCnt
    sc.stop()
    println(s"Static Nodes Count: $joinCnt, $cnt0, $cnt1")
    println(s"The algorithm is correct: ${flag}")
  }
}
