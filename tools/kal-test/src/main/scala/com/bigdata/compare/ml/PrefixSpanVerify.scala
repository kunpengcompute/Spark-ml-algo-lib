package com.bigdata.compare.ml

import io.airlift.compress.lz4.Lz4Codec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD

import java.io.FileWriter

object PrefixSpanVerify {
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

  def saveRes(res: RDD[String], savePath: String, sc: SparkContext): Unit ={
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val saveFile = new Path(savePath)
    if (fs.exists(saveFile)) {
      fs.delete(saveFile, true)
    }
    res.repartition(15).map((NullWritable.get(), _))
      .saveAsSequenceFile(savePath, Some(classOf[Lz4Codec]))

  }

  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val res1File = new Path(path0)
    val res2File = new Path(path1)
    if (fs.exists(res1File) && fs.exists(res2File)) {
      val res1 = sc.sequenceFile[NullWritable, String](path0).map(_._2).persist()
      val res2 = sc.sequenceFile[NullWritable, String](path1).map(_._2).persist()
      if (res2.subtract(res1).take(1).isEmpty &&
        res1.subtract(res2).take(1).isEmpty) {
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