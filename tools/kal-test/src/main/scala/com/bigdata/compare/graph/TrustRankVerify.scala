package com.bigdata.compare.graph

import org.apache.spark.{SparkConf, SparkContext}

object TrustRankVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val split = "\t"
    val sparkConf = new SparkConf().setAppName("TrustRankVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val rdd0 = sc.textFile(path0).map(s => {
      val arr = s.split(split)
      (arr(0).toInt, arr(1).toDouble)
    }).cache()
    val rdd1 = sc.textFile(path1).map(s => {
      val arr = s.split(split)
      (arr(0).toInt, arr(1).toDouble)
    }).cache()

    val cnt0 = rdd0.filter(_._2 > 0).count()
    val cnt1 = rdd1.filter(_._2 > 0).count()
    val statisticR = rdd0.join(rdd1).map(f => {
      if (f._2._2 != 0) {
        Math.abs(f._2._1  - f._2._2) / f._2._2
      } else {
        Math.abs(f._2._1  - f._2._2)
      }
    }).cache()
    val joinCnt = statisticR.count()
    val maxError = statisticR.max()
    val minError = statisticR.min()
    val flag = maxError <= 10e-7
    sc.stop()
    println(s"Static Nodes Count(>0): $joinCnt, $cnt0, $cnt1")
    println(s"Static Max relative Error: $maxError")
    println(s"Static Min relative Error: $minError")
    println(s"The algorithm is correct: ${flag}")
  }
}
