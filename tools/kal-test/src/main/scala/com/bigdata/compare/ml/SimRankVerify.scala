// scalastyle:off
package com.bigdata.compare.ml

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.FileWriter

object SimRankVerify {
  val EPS = 1e-7

  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)

    val conf = new SparkConf().setAppName("SimRankVerify")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val isCorrect = compareRes(path0, path1, spark)
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"${isCorrect}:\n${path0}\n${path1}\n")
    writerIsCorrect.close()
    println(s"${isCorrect}!")

    spark.stop()
  }

  def saveRes(res1: DataFrame, res2: DataFrame, savePath: String, sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val saveFile = new Path(savePath)
    if (fs.exists(saveFile)) {
      fs.delete(saveFile, true)
    }
    res1.foreach(_ => {})
    res2.foreach(_ => {})
    res1.write.mode("overwrite").option("header", value = true).csv(s"${savePath}/user")
    res2.write.mode("overwrite").option("header", value = true).csv(s"${savePath}/item")
  }

  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    var isCorrect = "correct"
    val res0UserPath = s"${path0}/user"
    val res0ItemPath = s"${path0}/item"
    val res1UserPath = s"${path1}/user"
    val res1ItemPath = s"${path1}/item"
    val res0User = spark.read.option("header", value = true).option("inferSchema", value = true).csv(res0UserPath)
    val res0Item = spark.read.option("header", value = true).option("inferSchema", value = true).csv(res0ItemPath)
    val res1User = spark.read.option("header", value = true).option("inferSchema", value = true).csv(res1UserPath)
    val res1Item = spark.read.option("header", value = true).option("inferSchema", value = true).csv(res1ItemPath)
    val userSim = res0User.join(res1User, Seq("user1", "user2"), "full")
    val itemSim = res0Item.join(res1Item, Seq("item1", "item2"), "full")
    userSim.foreach(row =>
    {if(math.abs(row.getDouble(2) - row.getDouble(3)) >= EPS)
      isCorrect = "incorrect"})
    itemSim.foreach(row =>
    {if(math.abs(row.getDouble(2) - row.getDouble(3)) >= EPS)
      isCorrect = "incorrect"})
    isCorrect
  }
}
