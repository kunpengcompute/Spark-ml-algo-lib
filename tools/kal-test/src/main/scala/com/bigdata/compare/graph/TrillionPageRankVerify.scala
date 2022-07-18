package com.bigdata.compare.graph

import org.apache.spark.{SparkConf, SparkContext}

object TrillionPageRankVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val split0 = args(2)
    val split1 = args(3)
    val sparkConf =  new SparkConf().setAppName("TrillionPageRankVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val rdd0 = sc.textFile(path0).map(s => {
      val arr = s.split(split0)
      (arr(0).toInt, arr(1).toDouble)
    }).cache()
    val rdd1 = sc.textFile(path1).map(s => {
      val arr = s.split(split1)
      (arr(0).toInt, arr(1).toDouble)
    }).cache()
    val cnt0 = rdd0.map(_._1).count()
    val cnt1 = rdd1.map(_._1).count()
    val statisticR = rdd0.join(rdd1).map(f => Math.abs(f._2._1 / cnt0 - f._2._2 / cnt1)).cache()
    val joinCnt = statisticR.count()
    val maxError = statisticR.max()
    val minError = statisticR.min()
    sc.stop()
    println(s"Static Nodes Count: $joinCnt, $cnt0, $cnt1")
    println(s"Static Max relative Error: $maxError")
    println(s"Static Min relative Error: $minError")
  }
}
