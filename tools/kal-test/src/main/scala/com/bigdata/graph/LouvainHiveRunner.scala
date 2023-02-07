package com.bigdata.graph

import org.apache.spark.graphx.lib.Louvain
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf

object LouvainHiveRunner {

  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      println("Usage:LouvainRunner <table_name> <col1> <col2> <colWeight> <iterNum> <isDirected> <partition> <save_mode> <save_arg>")
      System.exit(-1)
    }
    val tableName: String = args(0)
    val col1: String = args(1)
    val col2: String = args(2)
    val colWeight: String = args(3)
    val iterNum: Int = args(4).toInt
    val isDirected: Boolean = args(5).toBoolean
    val partition: Int = args(6).toInt
    val saveMode: String = args(7)
    val saveArg: String = args(8)

    try {
      val appName = s"Louvain_${tableName}"
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
      val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
      // record start time
      val startTime: Long = System.currentTimeMillis()

      val sql = s"select * from ${tableName}"
      var edgesRDD: RDD[(Long, Long, Double)] = null
      var verticesRDD: RDD[(String, Long)] = null
      if (colWeight == "none") {
        // 类型是string，转化为long
        val edges: DataFrame = spark.sql(sql).select(col1, col2)
        val tmpRDD: RDD[(String, String)] = edges.rdd.map(row => (row(0).toString, row(1).toString))
        verticesRDD = tmpRDD.flatMap(f => Iterator(f._1, f._2)).distinct.zipWithIndex().cache()
        edgesRDD = tmpRDD.leftOuterJoin(verticesRDD, partition).map(f => (f._2._1, f._2._2.get)).leftOuterJoin(verticesRDD, partition).map(f => (f._2._1, f._2._2.get, 1.0))
      } else {
        // 类型是string，转化为long
        val edges: DataFrame = spark.sql(sql).select(col1, col2, colWeight)
        val tmpRDD: RDD[(String, (String, Double))] = edges.rdd.map(row => (row(0).toString, (row(1).toString, row(2).toString.toDouble)))
        verticesRDD = tmpRDD.flatMap(f => Iterator(f._1, f._2._1)).distinct().zipWithIndex().cache()
        verticesRDD.foreachPartition(_ => {})
        println("vertices count:" + verticesRDD.count())
        edgesRDD = tmpRDD.leftOuterJoin(verticesRDD, partition).map(f => (f._2._1._1, (f._2._2.get, f._2._1._2))).leftOuterJoin(verticesRDD, partition).map(f => (f._2._1._1, f._2._2.get, f._2._1._2))
      }

      val (q, comm) = Louvain.run(edgesRDD, iterNum, isDirected, partition)
      // long to string
      val finalResult: RDD[Row] = comm.leftOuterJoin(verticesRDD.map(_.swap), 200).map(f => Row(f._2._2.get, f._2._1))

      val _ = saveMode match {
        case "hive" => {
          val schema_commMap: StructType = StructType(List(StructField("_node_id", StringType, true), StructField("_comm_id", LongType, true)))
          val commMapDF: DataFrame = spark.createDataFrame(finalResult, schema_commMap)
          commMapDF.createOrReplaceTempView("commMapTmpV")
          val outputTableName: String = saveArg
          val createSql = s"create table ${outputTableName}_louvain(_node_id varchar(250), _comm_id long)"
          spark.sql(createSql)
          spark.sql(s"insert into ${outputTableName}_louvain select * from commMapTmpV")
        }
        case "hdfs" => {
          val outputPath: String = saveArg
          Util.saveDataToHDFS(comm, ",", outputPath)
        }
        case _ => throw new Exception("illegal save mode")
      }

      val costTime: Double = (System.currentTimeMillis() - startTime) / 1000.0
      val commNum: Int = comm.map(_._2).distinct().count().toInt

      println("louvain modularity:\t%.5f\nComm num:\t%d\n".format(q, commNum))
      println(s"Exec Successful: costTime: ${costTime}s")
      spark.close()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
