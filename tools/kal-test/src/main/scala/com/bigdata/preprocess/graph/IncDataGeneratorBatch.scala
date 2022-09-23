package com.bigdata.preprocess.graph

import com.bigdata.graph.Util

import org.apache.spark.graphx.lib.TrillionPageRank
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}

object IncDataGeneratorBatch {
  def main(args: Array[String]): Unit = {
    val host = args(0) // local or yarn
    val inputPath = args(1)
    val split = args(2)
    val outputPath = args(3)
    val rate = args(4).toDouble
    val partition = args(5).toInt
    val seed = args(6).toLong
    val iterNum = args(7).toInt
    val resetProb = args(8).toDouble
    val batchNum = args(9).toInt

    val sparkConf = new SparkConf().setAppName(s"IncDataGenerator-${rate}_batch_${batchNum}").setMaster(host)
    sparkConf.set("spark.sql.orc.impl", "native")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val schema = StructType(
      Seq(
        StructField("srcId", LongType, true),
        StructField("srcStatus", IntegerType, true),
        StructField("dstId", ArrayType(LongType), true),
        StructField("dstStatus", ArrayType(IntegerType), true),
        StructField("pr", DoubleType, true)
      )
    )
    val partitioner = new HashPartitioner(partition)
    var allGraph = Util.readEdgeList(sc, inputPath, "\\s+", partition)
      .partitionBy(partitioner)
      .setName("batch-allGraph")
      .persist(StorageLevel.MEMORY_ONLY_SER)

    for (bn <- 1 to batchNum reverse) {
      val allNodes = allGraph.keys.union(allGraph.values).distinct().persist(StorageLevel.MEMORY_ONLY_SER)
      val allSrcNodes = allGraph.keys.distinct().persist(StorageLevel.MEMORY_ONLY_SER)
      val incNodesWithTag = allSrcNodes.sample(false, rate, seed)
        .map(f => (f, 1))
        .persist(StorageLevel.MEMORY_ONLY_SER)
      allNodes.unpersist(false)
      allSrcNodes.unpersist(false)

      val nodeWithStatus = allGraph.leftOuterJoin(incNodesWithTag, partitioner).map(f => {
        (f._2._1, (f._1, f._2._2.getOrElse(0)))
      }).leftOuterJoin(incNodesWithTag, partitioner).map(f => {
        (f._2._1._1, Array((f._2._1._2, f._1, f._2._2.getOrElse(0))))
      })
        .setName("nodeWithStatus")
        .persist(StorageLevel.MEMORY_ONLY_SER)
      nodeWithStatus.foreachPartition(f => {})
      allGraph.unpersist(false)
      incNodesWithTag.unpersist(false)

      val incGraph0to1 = nodeWithStatus.filter(f => {
        val a = f._2(0)
        a._1 == 0 && a._3 == 1
      })
      val incGraph1to0 = nodeWithStatus.filter(f => {
        val a = f._2(0)
        a._1 == 1 && a._3 == 0
      })
      val incGraph1to1 = nodeWithStatus.filter(f => {
        val a = f._2(0)
        a._1 == 1 && a._3 == 1
      })

      val srcLinks = nodeWithStatus.reduceByKey(_++_).mapValues(f => {
        val srcStatus = f(0)._1
        val dstArr = f.map(x => x._2)
        val dstStatusArr = f.map(x => x._3)
        (srcStatus, dstArr, dstStatusArr)
      })
        .setName("srcLinks")
        .persist(StorageLevel.MEMORY_ONLY_SER)
      srcLinks.foreachPartition(f => {})
      nodeWithStatus.unpersist(false)
      val orgGraph = srcLinks.filter(f => f._2._1 == 0).map(f => {
        val dstArr = f._2._2.zip(f._2._3)
        val dstMap = collection.mutable.Map(dstArr: _ *)
        val dstIncMap = dstMap.retain((k, v) => (v == 0))
        (f._1, (1.0, dstIncMap.keys.toArray))
      }).filter(f => f._2._2.nonEmpty)
        .setName("orgGraph")
        .persist(StorageLevel.MEMORY_ONLY_SER)
      orgGraph.foreachPartition(f => {})

      val orgEges = orgGraph.map(f => f._2._2.length)
      val orgPr = TrillionPageRank.run(orgGraph, partition, iterNum, resetProb, false)
      val incData = srcLinks.leftOuterJoin(orgPr, partitioner).map(f =>
        Row(f._1, f._2._1._1, f._2._1._2, f._2._1._3, f._2._2.getOrElse(1.0))
      ).setName("incData")
        .persist(StorageLevel.MEMORY_ONLY_SER)
      incData.foreachPartition(f => {})
      srcLinks.unpersist(false)
      orgPr.unpersist(false)

      val incDataDF = spark.createDataFrame(incData, schema)
      incDataDF.write.option("header", "true").mode(SaveMode.Overwrite).orc(outputPath + s"_batch_${bn}")
      allGraph = orgGraph.map(f => (f._1, f._2._2)).flatMapValues(f => f)
    }
  }
}
