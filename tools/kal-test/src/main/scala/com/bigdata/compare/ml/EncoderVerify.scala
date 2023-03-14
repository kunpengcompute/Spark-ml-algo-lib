package com.bigdata.compare.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.FileWriter

object EncoderVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val sparkConf =  new SparkConf().setAppName("DownUpEvaluationVerify")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val isCorrect = compareRes(path0, path1, spark)
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"${isCorrect}:\n${path0}\n${path1}\n")
    writerIsCorrect.close()
    println(s"The algorithm is correct: ${isCorrect}")

  }

  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val res1File = new Path(path0)
    val res2File = new Path(path1)
    if (!fs.exists(res1File) || !fs.exists(res2File)) {
      return "invaildComparison"
    }
    val data1 = spark.read.parquet(path0)
    val data2 = spark.read.parquet(path1)
    data1.show()
    val rdd1 = data1.rdd.map{
      r =>
        val field1 = r.getAs[Seq[Int]]("7xxx_index")(0)
        val field2 = r.getAs[Seq[Int]]("15xxx_index")(0)
        (field1, field2)
    }
    val rdd2 = data2.rdd.map{
      r =>
        val field1 = r.getAs[Seq[Int]]("7xxx_index")(0)
        val field2 = r.getAs[Seq[Int]]("15xxx_index")(0)
        (field1, field2)
    }
    val diff = rdd1.subtract(rdd2).count()
    println(s"Exec Successful: different count: ${diff}")
    if (diff == 0) {
      return "true"
    }
    else {
      return "false"
    }

  }

}