package com.bigdata.compare.graph

import org.apache.spark.{SparkConf, SparkContext}

object PersonalizedPageRankVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val api = args(2)
    val src = args(3)
    val split = "\t"
    val sparkConf = new SparkConf().setAppName("PersonalizedPageRankVerify")
    val sc = SparkContext.getOrCreate(sparkConf)

    var maxError = 0.0
    var minError = 1.0
    api match {
      case "fixMS" =>
        val rdd0 = sc.textFile(path0).map(s => {
          val arr = s.split(split)
          val len = arr(1).length
          val err = arr(1).substring(1, len - 1)
          (arr(0).toInt, err)
        }).cache()
        val rdd1 = sc.textFile(path1).map(s => {
          val arr = s.split(split)
          val arr1 = arr(1).split(",\\[")
          val err = arr1(2).dropRight(2)
          (arr(0).toInt, err)
        }).cache()
        val statisticR = rdd0.join(rdd1).map(f => {
          val err1 = f._2._1.split(",")
          val err2 = f._2._2.split(",")
          var min = 1.0
          var max = 0.0
          for (i <- 0 until src.toInt) {
            var err = 0.0
            if (err2(i).toDouble == 0) {
              err = Math.abs(err1(i).toDouble - err2(i).toDouble)
            } else {
              err = Math.abs(err1(i).toDouble - err2(i).toDouble) / err2(i).toDouble
            }
            min = Math.min(min, err)
            max = Math.max(max, err)
          }
          (min, max)
        }).cache()
        minError = statisticR.keys.min()
        maxError = statisticR.values.max()
      case _ =>
        val rdd0 = sc.textFile(path0).map(s => {
          val arr = s.split(split)
          (arr(0).toInt, arr(1).toDouble)
        }).cache()
        val rdd1 = sc.textFile(path1).map(s => {
          val arr = s.split(split)
          (arr(0).toInt, arr(1).toDouble)
        }).cache()

        val statisticR = rdd0.join(rdd1).map(f => {
          if (f._2._2 == 0) {
            Math.abs(f._2._1  - f._2._2)
          } else {
            Math.abs(f._2._1  - f._2._2) / f._2._2
          }
        }).cache()
        maxError = statisticR.max()
        minError = statisticR.min()
    }

    val flag = maxError <= 10e-7
    sc.stop()
    println(s"Static Max relative Error: $maxError")
    println(s"Static Min relative Error: $minError")
    println(s"The algorithm is correct: ${flag}")
  }
}
