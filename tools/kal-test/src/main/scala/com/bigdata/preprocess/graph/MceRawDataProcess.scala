package com.bigdata.preprocess.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object MceRawDataProcess extends Serializable {
  private val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK_SER
  val maxDegree = 2000

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val sparkConf = new SparkConf().setAppName("MceRawDataProcess")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val split = " "
    val partition = 284
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()
    val edgeList = readUndirectDataFromHDFS(sc, input, split, partition)
    val (newNodeID, resRDD) = reIdNodes(edgeList, maxDegree)
    val numNodes = newNodeID.map(_._2).max() + 1
    val numEdge = resRDD.count()
    resRDD.map(f => f._1 + "," + f._2).repartition(1).saveAsTextFile(output)
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Exec Successful:numNodes:${numNodes},numEdge:${numEdge},MceRawDataProcess costTime:${costTime}s")
  }

  private def reIdNodes[T: ClassTag](graph: RDD[(T, T)], maxDegree: Int): (RDD[(Int, T)], RDD[(Int, Int)]) = {
    val tempInput = graph.flatMap(f => {
      val ret = f._1.toString.compareTo(f._2.toString)
      if (ret == 0) {
        Iterator.empty
      } else if (ret < 0) {
        Iterator((f._1, f._2))
      } else {
        Iterator((f._2, f._1))
      }
    }).distinct().persist(DEFAULT_STORAGE_LEVEL)

    tempInput.foreachPartition(_ => {})

    val newNodeId = tempInput.flatMap(f => Iterator((f._1, 1), (f._2, 1)))
      .reduceByKey((x, y) => x + y)
      .filter(f => f._2 != 1&& f._2 < maxDegree)
      .sortBy(f => f._2)
      .zipWithIndex().map(f => (f._1._1, f._2.toInt))
      .persist(DEFAULT_STORAGE_LEVEL)

    newNodeId.foreachPartition(_ => {})

    val resEdgeRDD = tempInput.join(newNodeId).map(f => f._2).join(newNodeId).map(f => f._2)
    val resRevRdgeRDD = resEdgeRDD.map(f => (f._2, f._1))
    val resRDD = resEdgeRDD.union(resRevRdgeRDD)

    (newNodeId.map(f => (f._2, f._1)), resRDD)
  }

  def readUndirectDataFromHDFS(sc: SparkContext,
                               filePath: String,
                               split: String,
                               partition: Int): RDD[(Long, Long)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          val x = line.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            val node1 = x(0).toLong
            val node2 = x(1).toLong
            Iterator((node1, node2))
          }
        }
      })
  }
}