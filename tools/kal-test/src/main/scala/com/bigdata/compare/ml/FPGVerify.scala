package com.bigdata.compare.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

import java.io.FileWriter

object FPGVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val sparkConf =  new SparkConf().setAppName("FPGVerify")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val isCorrect = compareRes(path0, path1, spark)
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"${isCorrect}:\n${path0}\n${path1}\n")
    writerIsCorrect.close()
    println(s"The algorithm is correct: ${isCorrect}")

  }

  def saveRes(df: Dataset[String], saveDataPath: String, sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val saveFile = new Path(saveDataPath)
    if (fs.exists(saveFile)) {
      fs.delete(saveFile, true)
    }
    df.rdd.saveAsTextFile(saveDataPath)
  }

  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val res1File = new Path(path0)
    val res2File = new Path(path1)
    if (fs.exists(res1File) && fs.exists(res2File)) {
      val res1 = sc.textFile(path0)
      val res2 = sc.textFile(path1)
      if (res1.subtract(res2).take(1).isEmpty &&
        res2.subtract(res1).take(1).isEmpty ) {
        return "correct"
      } else {
        return "incorrect"
      }
    }
    else{
      return "invaildComparison"
    }
  }

}