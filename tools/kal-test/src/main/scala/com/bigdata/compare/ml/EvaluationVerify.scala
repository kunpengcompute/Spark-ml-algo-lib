package com.bigdata.compare.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.FileWriter

object EvaluationVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val sparkConf =  new SparkConf().setAppName("EvaluationVerify")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val isCorrect = compareRes(path0, path1, spark)
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"${isCorrect}:\n${path0}\n${path1}\n")
    writerIsCorrect.close()
    println(s"${isCorrect}!")

  }

  def saveRes(res: Double, savePath: String, sc: SparkContext): Unit ={
    val result = new Array[String](1)
    result(0) = res.toString
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val saveFile = new Path(savePath)
    if (fs.exists(saveFile)) {
      fs.delete(saveFile, true)
    }
    sc.parallelize(result).repartition(1).saveAsTextFile(savePath)
  }

  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val res1File = new Path(path0)
    val res2File = new Path(path1)
    if (fs.exists(res1File) && fs.exists(res2File)) {
      val res1 = sc.textFile(path0).collect()
      val res2 = sc.textFile(path1).collect()
      if (math.abs(res1(0).toDouble - res2(0).toDouble) / res1(0).toDouble <= 0.005) {
        return "correct"
      }
      else {
        return "incorrect"
      }
    }
    else{
      return "invaildComparison"
    }
  }
}