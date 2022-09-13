package com.bigdata.compare.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.FileWriter

object EvaluationVerify {
  def main(args: Array[String]): Unit = {
    val algorithmType = args(0)
    val path0 = args(1)
    val path1 = args(2)
    val sparkConf =  new SparkConf().setAppName("EvaluationVerify")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val isCorrect = compareRes(path0, path1, algorithmType, spark)
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"${isCorrect}:\n${path0}\n${path1}\n")
    writerIsCorrect.close()
    println(s"${isCorrect}!")

  }

  def compareRes(
                  path0: String,
                  path1: String,
                  algorithmType: String,
                  spark: SparkSession): String = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val res1File = new Path(path0)
    val res2File = new Path(path1)
    if (!fs.exists(res1File) || !fs.exists(res2File)) {
      return "invaildComparison"
    }
    val res1 = sc.textFile(path0).collect().head.toDouble
    val res2 = sc.textFile(path1).collect().head.toDouble
    if(algorithmType.equals("classification")) {
      return claCompare(res1, res2)
    }
    else {
      return regCompare(res1, res2)
    }
  }

  def claCompare(res1: Double, res2: Double) : String = {
    if (res1 * (1 + 0.005) - res2 >= 0.0) {
      return "correct"
    }
    else {
      return "incorrect"
    }
  }

  def regCompare(res1: Double, res2: Double) : String = {
    if (res1 * (1 - 0.005) - res2 <= 0.0) {
      return "correct"
    }
    else {
      return "incorrect"
    }
  }

}