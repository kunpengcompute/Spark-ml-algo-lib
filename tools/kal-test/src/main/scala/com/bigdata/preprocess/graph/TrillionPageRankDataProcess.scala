package com.bigdata.preprocess.graph

import org.apache.spark.{SparkConf, SparkContext}

object TrillionPageRankDataProcess {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val sparkConf = new SparkConf().setMaster("yarn")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(input, 300).map(f => {
      val tmp = f.split("\t")
      (tmp(0), Array(tmp(1)))
    })
    val adj = data.reduceByKey(_ ++ _)
    adj.map(f => f._1 + "_1.0," + f._2.mkString(",")).saveAsTextFile(output)
  }
}
