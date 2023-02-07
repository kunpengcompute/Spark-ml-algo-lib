package com.bigdata.graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf

object PageRankHiveRunner {

  def main(args: Array[String]): Unit = {
    if (args.length < 10) {
      println("Usage:PageRankRunner <tableName> <col1> <col2> <api> <tol> <resetProb> <numIter> <partition> <save_mode> <save_arg>")
      System.exit(-1)
    }
    val tableName: String = args(0)
    val col1: String = args(1)
    val col2: String = args(2)
    val api: String = args(3)
    val tol: Double = args(4).toDouble
    val resetProb: Double = args(5).toDouble
    val numIter: Int = args(6).toInt
    val partition: Int = args(7).toInt
    val saveMode: String = args(8)
    val saveArg: String = args(9)

    try {
      val appName = s"PageRank_${tableName}_${api}"
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
        .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

      // record start time
      val startTime: Long = System.currentTimeMillis()

      val sql = s"select * from ${tableName}"
      val edges: DataFrame = spark.sql(sql).select(col1, col2)
      val edgesRDD: RDD[(String, String)] = edges.rdd.map(row => (row(0).toString, row(1).toString))
      val verticesRDD: RDD[(String, Long)] = edgesRDD.flatMap(f => Iterator(f._1, f._2)).distinct.zipWithIndex().cache()
      verticesRDD.foreachPartition(_ => {})
      println("vertices count:" + verticesRDD.count())
      val indexEdgesRDD: RDD[Edge[Double]] = edgesRDD.leftOuterJoin(verticesRDD, partition).map(f => (f._2._1, f._2._2.get)).leftOuterJoin(verticesRDD, partition).map(f => Edge(f._2._1, f._2._2.get, 1.0))

      val graph: Graph[Double, Double] = Graph.fromEdges(indexEdgesRDD, 1.0)
      val result: Graph[Double, Double] = api match {
        case "runUntilConvergence" =>
          PageRank.runUntilConvergence(graph, tol, resetProb)
        case "run" => PageRank.run(graph, numIter, resetProb)
        case _ => throw new Exception("illegal api")
      }
      val resultRDD = result.vertices.map(f => (f._1, f._2))
      val finalResult: RDD[Row] = resultRDD.leftOuterJoin(verticesRDD.map(_.swap), partition).map(f => Row(f._2._2.get, f._2._1))


      val _ = saveMode match {
        case "hive" => {
          val schema_resultMap: StructType = StructType(List(StructField("_node_id", StringType, true), StructField("_pagerank", DoubleType, true)))
          val resultMapDF: DataFrame = spark.createDataFrame(finalResult, schema_resultMap)
          resultMapDF.createOrReplaceTempView("PageRankMapTmpV")
          val outputTableName: String = saveArg
          val createSql = s"create table ${outputTableName}_pagerank(_node_id varchar(250), _pagerank double)"
          spark.sql(createSql)
          spark.sql(s"insert into ${outputTableName}_pagerank select * from PageRankMapTmpV")
        }
        case "hdfs" => {
          val outputPath: String = saveArg
          result.vertices.map(f => s"${f._1}\t${f._2}").saveAsTextFile(outputPath)
        }
        case _ => throw new Exception("illegal save mode")
      }

      val costTime: Double = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Exec Successful: costTime: ${costTime}s")
      spark.close()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
