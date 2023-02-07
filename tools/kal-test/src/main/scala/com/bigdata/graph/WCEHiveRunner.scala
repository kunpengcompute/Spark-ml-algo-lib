package com.bigdata.graph

import org.apache.spark.graphx.lib.WeakCliqueEnumeration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf

object WCEHiveRunner {

  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      println("Usage:WCERunner <table name> <col1> <col2> <maxIter> <maxDegree> <save_mode> <save_arg>")
      System.exit(-1)
    }

    val tableName: String = args(0)
    val col1: String = args(1)
    val col2: String = args(2)
    val maxIter: Int = args(3).toInt
    val maxDegree: Int = args(4).toInt
    val saveMode: String = args(5)
    val saveArg: String = args(6)

    try {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName("WCE")
        .setMaster("yarn")
        .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

      // record start time
      val startTime: Long = System.currentTimeMillis()
      val sql = s"select * from ${tableName}"
      val edges: DataFrame = spark.sql(sql).select(col1, col2)
      val edgesRDD: RDD[(String, String)] = edges.rdd.map(row => (row(0).toString, row(1).toString))

      val result: (RDD[(Int, String)], RDD[(Int, Int)]) = WeakCliqueEnumeration.run(edgesRDD, maxIter, maxDegree)
      val _ = saveMode match {
        case "hive" => {
          val nodeMap: RDD[Row] = result._1.asInstanceOf[RDD[(Int, String)]].map(t => Row(t._2, t._1))
          val cliqueMap: RDD[Row] = result._2.asInstanceOf[RDD[(Int, Int)]].map(t => Row(t._1, t._2))
          val schema_nodeMap: StructType = StructType(List(StructField("_id", StringType, true), StructField("_id_int", IntegerType, true)))
          val schema_cliqueMap: StructType = StructType(List(StructField("_id_int", IntegerType, true), StructField("_clique_id", IntegerType, true)))

          val nodeMapDF: DataFrame = spark.createDataFrame(nodeMap, schema_nodeMap)
          val cliqueMapDF: DataFrame = spark.createDataFrame(cliqueMap, schema_cliqueMap)
          nodeMapDF.createOrReplaceTempView("nodeMapTmpV")
          cliqueMapDF.createOrReplaceTempView("cliqueMapTmpV")

          val outputTableName: String = saveArg
          val createSql = s"create table ${outputTableName}_wce_nodeMap(_id varchar(250), _id_int int)"
          val createSql2 = s"create table ${outputTableName}_wce_cliqueMap(_id_int int,_clique_id int)"
          spark.sql(createSql)
          spark.sql(createSql2)

          spark.sql(s"insert into ${outputTableName}_wce_nodeMap select * from nodeMapTmpV")
          spark.sql(s"insert into ${outputTableName}_wce_cliqueMap select * from cliqueMapTmpV")
        }
        case "hdfs" => {
          val outputPath: String = saveArg
          Util.saveDataToHDFS(result._1, ",", outputPath + "/map_info")
          Util.saveDataToHDFS(result._2, ",", outputPath + "/clique_info")
        }
        case _ => throw new Exception("illegal save mode")
      }

      val costTime: Double = (System.currentTimeMillis() - startTime) / 1000.0
      println("weak clique detection result = true")
      println(s"Exec Successful: costTime: ${costTime}s")
      spark.close()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
