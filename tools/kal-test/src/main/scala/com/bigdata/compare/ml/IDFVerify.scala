package com.bigdata.compare.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.FileWriter

object IDFVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val sparkConf =  new SparkConf().setAppName("IDFVerify")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val isCorrect = compareRes(path0, path1, spark)
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"${isCorrect}:\n${path0}\n${path1}\n")
    writerIsCorrect.close()
    println(s"${isCorrect}!")

  }

  def saveRes(res: Array[Double], saveDataPath: String, sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val saveFile = new Path(saveDataPath)
    if (fs.exists(saveFile)) {
      fs.delete(saveFile, true)
    }
    sc.parallelize(res).repartition(100).saveAsTextFile(saveDataPath)
  }

  def isEqualRes(res1: Array[Double], res2: Array[Double]): Boolean = {
    if (res1.length != res2.length)
      return false
    for (i <- res2.indices) {
      if (math.abs(res1(i) - res2(i)) > 1e-6)
        return false
    }
    true
  }

  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val res1File = new Path(path0)
    val res2File = new Path(path1)
    if (fs.exists(res1File) && fs.exists(res2File)) {
      val res1 = sc.textFile(path0).map(_.toDouble).collect().toArray
      val res2 = sc.textFile(path1).map(_.toDouble).collect().toArray
      val numMismatch = res2.seq.zip(res1).count(v => math.abs(v._1 - v._2) > 1E-1)
      if (numMismatch != 0 ) {
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