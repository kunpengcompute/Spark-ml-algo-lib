package com.bigdata.compare.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector

import java.io.FileWriter

object TEVerify {
  val TOLERANCE = 1E-15

  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val sparkConf = new SparkConf().setAppName("TEVerify")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val isCorrect = compareRes(path0, path1, spark)
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"${isCorrect}:\n${path0}\n${path1}\n")
    writerIsCorrect.close()
    println(s"The algorithm is correct: ${isCorrect}")

  }

  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    var isCorrect = "true"
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val encodedTrainSavePath = s"${path0}/encodedTrain"
    val encodedTestSavePath = s"${path0}/encodedTest"
    val h2oEncodedTrainSavePath = s"${path1}/encodedTrain"
    val h2oEncodedTestSavePath = s"${path1}/encodedTest"
    val optTrainFile = new Path(encodedTrainSavePath)
    val optTestFile = new Path(encodedTestSavePath)
    val rawTrainFile = new Path(h2oEncodedTrainSavePath)
    val rawTestFile = new Path(h2oEncodedTestSavePath)
    if (!fs.exists(optTrainFile) || !fs.exists(optTestFile) || !fs.exists(rawTrainFile) || !fs.exists(rawTestFile)) {
      isCorrect = "invaildComparison"
      return isCorrect
    }
    println("Start functionality verification")
    println("loading parquet1")
    val trainDF1 = spark.read.parquet(encodedTrainSavePath)
    val colsToTrainCompare = trainDF1.columns.filter(_.endsWith("_te"))
    println("collecting encodings1")
    val encoded1 = colsToTrainCompare.map(col => trainDF1.select(col).rdd.map(_.getAs[Vector](0).toArray(0)).collect().sorted)

    println("loading parquet2")
    val trainDF2 = spark.read.parquet(h2oEncodedTrainSavePath)
    println("collecting encodings2")
    val encoded2 = colsToTrainCompare.map(col => trainDF2.select(col).rdd.map(_.getAs[Vector](0).toArray(0)).collect().sorted)
    if (encoded1.length != encoded2.length) {
      isCorrect = "false"
      return isCorrect
    }
    encoded1.zip(encoded2).map { xy =>
      xy._1.zip(xy._2).map { ab =>
        if (math.abs(ab._1 - ab._2) > TOLERANCE) {
          isCorrect = "false"
          return isCorrect
        }
      }
    }
    println("Train results all close.")

    val testDF1 = spark.read.parquet(encodedTestSavePath)
    val testDF2 = spark.read.parquet(h2oEncodedTestSavePath)
    val colsToTestCompare = testDF1.columns.filter(_.endsWith("_te"))
    for (col <- colsToTestCompare) {
      val res1 = testDF1.select(col).rdd.map(_.getAs[Vector](0).toArray(0)).collect().sorted
      val res2 = testDF2.select(col).rdd.map(_.getAs[Vector](0).toArray(0)).collect().sorted
      if (res1.length != res2.length) {
        isCorrect = "false"
        return isCorrect
      }
      res1.zip(res2).map { ab =>
        if (math.abs(ab._1 - ab._2) > TOLERANCE) {
          isCorrect = "false"
        }
      }
    }
    println("Test results all close.")
    println("Functionality OK")
    isCorrect
  }
}