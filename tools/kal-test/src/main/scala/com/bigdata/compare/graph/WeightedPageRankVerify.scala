package com.bigdata.compare.graph

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object WeightedPageRankVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0) //origin path
    val path1 = args(1) //opt path

    val sparkConf = new SparkConf().setAppName("WeightedPageRankVerify")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val toDUDF = udf((s: String) => s.toDouble)
    val toLUDF = udf((s: String) => s.toLong)
    val data0 = spark.read.parquet(path0)
    val data1 = spark.read.option("SEP", ",").csv(path1)
      .withColumn("pr1", toDUDF(col("_c1")))
      .withColumn("_c0", toDUDF(col("_c0")))
    val result = data0.join(data1, data0("id") === data1("_c0"))
    val toDiff = udf((i1: Double, i2: Double) => Math.abs(i1 - i2) / i1)
    val summ = result.withColumn("diff", toDiff(col("value"), col("pr1"))).cache()
    summ.select("diff").summary().show(false)
    val maxError = summ.select("diff").rdd.map(r => r.getAs[Double]("diff")).max()
    val flag = maxError <= 1e-7
    println(s"The algorithm is correct: ${flag}")
  }
}
