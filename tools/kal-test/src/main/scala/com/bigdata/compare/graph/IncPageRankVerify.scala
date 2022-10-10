package com.bigdata.compare.graph

import java.text.DecimalFormat

import scala.collection.mutable

import org.apache.commons.math3.distribution.NormalDistribution

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object IncPageRankVerify {
  val filterFactor = 0.15
  val numLevels = 100
  val start = -1.0
  val end = 5.0

  def getLevelArray(sc: SparkContext, numNodes: Long): Broadcast[Array[Long]] = {
    val levelArray = new Array[Long](numLevels + 1)
    val posDis = new NormalDistribution(0, 1.0)
    val totalDiff = posDis.cumulativeProbability(end) - posDis.cumulativeProbability(start)
    val step = (end - start) / numLevels
    for (i <- 0 until numLevels) {
      val diff = posDis.cumulativeProbability(-1 + (i + 1) * step) - posDis.cumulativeProbability(-1 + i * step)
      val interval = math.floor((diff / totalDiff) * numNodes).toLong
      levelArray(i + 1) = interval + levelArray(i)
    }
    levelArray(numLevels) = numNodes + 1
    sc.broadcast(levelArray)
  }

  def getLevel(sc: SparkContext, prRdd: RDD[(Long, Double)]): RDD[(Long, Int)] = {
    val finalData = prRdd.filter(_._2 > filterFactor)
    val finalDataNum = finalData.count()
    val finalData0 = prRdd.filter(_._2 == filterFactor).map(f => (f._1, 0))
    val finalLevelArray = getLevelArray(sc, finalDataNum)
    val level = finalData.map{
      case (node, pr) => (math.log(pr - filterFactor), node)
    }
      .sortBy(v => v)
      .zipWithIndex()
      .map{
        case ((_, node), index) => (node, finalLevelArray.value.indexWhere(_ >= index))
      }
      .union(finalData0)
    level.foreachPartition(f => {})
    prRdd.unpersist(false)
    level
  }

  def getFullLevel(sc: SparkContext,
                   fullResult: RDD[(Long, Double)],
                   vertexWithStatus: RDD[(Long, (Int, Double))],
                   partition: Int,
                   storageLevel: StorageLevel): RDD[(Long, Int)] = {
    val incPr = vertexWithStatus.map(f => (f._1, f._2._1))
      .zipPartitions(fullResult, true)((tagIter, prIter) => {
        val tagMap = tagIter.toMap
        val prMap = prIter.toMap
        val map = new mutable.HashMap[Long, (Int, Double)]()
        tagMap.keys.foreach(key => {
          map += (key -> (tagMap.getOrElse(key, 2), prMap.getOrElse(key, 0.0)))
        })
        map.toIterator
    })
      .filter(f => f._2._1 == 1)
      .map(f => (f._1, f._2._2))
      .persist(storageLevel)
    incPr.foreachPartition(f => {})
    fullResult.unpersist(false)

    val orgPr = vertexWithStatus.filter(f => f._2._1 == 0).map(f => (f._1, f._2._2)).persist(storageLevel)
    orgPr.foreachPartition(f => {})
    val prRdd = incPr.union(orgPr).persist(storageLevel)
    prRdd.foreachPartition(f => {})
    incPr.unpersist(false)
    orgPr.unpersist(false)

    getLevel(sc, prRdd)
  }

  def getIncLevel(sc: SparkContext,
                   incResult: RDD[(Long, Double)],
                   vertexWithStatus: RDD[(Long, (Int, Double))],
                   partition: Int,
                   storageLevel: StorageLevel): RDD[(Long, Int)] = {
    val orgPr = vertexWithStatus.filter(f => f._2._1 == 0).map(f => (f._1, f._2._2)).persist(storageLevel)
    orgPr.foreachPartition(f => {})
    val prRdd = incResult.union(orgPr).persist(storageLevel)
    prRdd.foreachPartition(f => {})
    orgPr.unpersist(false)

    getLevel(sc, prRdd)
  }

  def getAccuracy(sc: SparkContext,
                  graph: RDD[(Long, (Int, Double))],
                  fullResult: RDD[(Long, Double)],
                  incResult: RDD[(Long, Double)],
                  numPart: Int,
                  outputPath: String = ""): Double = {
    val incIndex = getIncLevel(sc, incResult, graph, numPart, StorageLevel.MEMORY_ONLY_SER)
    val fullIndex = getFullLevel(sc, fullResult, graph, numPart, StorageLevel.MEMORY_ONLY_SER)
    val eval = fullIndex.leftOuterJoin(incIndex).mapValues {
      case (gt, res) => res.fold(-1)(v => {
        val score = math.abs(gt - v)
        if (score > 10) -2 else score
      })
    }.map(v => (v._2, 1L)).reduceByKey(_ + _)

    val sum = eval.values.sum()
    if (outputPath.length != 0) {
      eval.repartition(1).sortBy(_._1)
        .map(f => f._1 + "," + f._2 + "," + (f._2 / sum).formatted("%.6f"))
        .saveAsTextFile(outputPath)
    }

    val accNum = eval.filter(f => f._1 == 0 || f._1 == 1).values.sum()
    accNum / sum
  }

  def main(args: Array[String]): Unit = {
    val master = args(0)
    val incPrPath = args(1)
    val gdPrPath = args(2)
    val split = args(3)
    val numPart = args(4).toInt
    val outputPath = args(5)
    val graphPath = args(6)

    val sparkConf = new SparkConf().setAppName("IncPageRankVerify").setMaster(master)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext

    val graph = spark.read
      .orc(graphPath)
      .rdd
      .map(row => (row.getAs[Long]("srcId"),
      (row.getAs[Int]("srcStatus"),
      row.getAs[Double]("pr"))))
      .setName("graph")
      .partitionBy(new HashPartitioner(numPart))
      .persist(StorageLevel.MEMORY_ONLY_SER)
    graph.foreachPartition(f => {})

    val incResult = sc.textFile(incPrPath).flatMap(line => {
      val items = line.trim.split(split)
      Iterator.single((items(0).toLong), items(1).toDouble)
    }).partitionBy(new HashPartitioner(numPart))
      .persist(StorageLevel.MEMORY_ONLY_SER)
    incResult.foreachPartition(f => {})

    val fullResult = sc.textFile(gdPrPath).flatMap(line => {
      val items = line.trim.split(split)
      Iterator.single((items(0).toLong), items(1).toDouble)
    }).partitionBy(new HashPartitioner(numPart))
      .persist(StorageLevel.MEMORY_ONLY_SER)
    fullResult.foreachPartition(f => {})

    val acc = getAccuracy(sc, graph, fullResult, incResult, numPart, outputPath)
    val df = new DecimalFormat("#.00")
    val flag = df.format(acc).toDouble >= 0.96
    spark.stop()
    println("accuracy: ", acc)
    println(s"The algorithm is correct: ${flag}")
  }
}
