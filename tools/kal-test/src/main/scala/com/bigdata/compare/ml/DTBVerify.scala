package com.bigdata.compare.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.FileWriter

object DTBVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val sparkConf =  new SparkConf().setAppName("DTBVerify")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val isCorrect = compareRes(path0, path1, spark)
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"${isCorrect}:\n${path0}\n${path1}\n")
    writerIsCorrect.close()
    println(s"${isCorrect}!")

  }

  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val res1File = new Path(path0)
    val res2File = new Path(path1)
    if (!fs.exists(res1File) || !fs.exists(res2File)) {
      return "invaildComparison"
    }

    val output = sc.textFile(path0).repartition(100)
    val refRes = sc.textFile(path1).repartition(100)
    val dataDiff1Cnt = output.subtract(refRes).count()
    val dataDiff2Cnt = refRes.subtract(output).count()
    if (dataDiff1Cnt != 0 || dataDiff2Cnt != 0) {
      System.err.println(s"[ERROR] diff1Cnt: ${dataDiff1Cnt}, diff2Cnt: ${dataDiff2Cnt}")
      System.err.println("output data is mismatch!")
      return "incorrect"
    } else {
      return "correct"
    }
  }
}