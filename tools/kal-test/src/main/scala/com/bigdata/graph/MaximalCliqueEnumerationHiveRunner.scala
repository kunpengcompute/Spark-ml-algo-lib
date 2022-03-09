package com.bigdata.graph

import org.apache.spark.SparkConf
import org.apache.spark.graphx.lib.MaximalCliqueEnumeration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, StringType}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object MaximalCliqueEnumerationHiveRunner {
  private val MCE_OUTPUT_SPLIT = ","

  def main(args: Array[String]): Unit = {
    if (args.length < 8) {
      println("Usage:MaxcliqueEnumerationRunner <table_name> <col1> <col2> <mink> <maxDegree> <partition> <save_mode> <save_arg>")
      System.exit(-1)
    }
    val tableName = args(0)
    val col1 = args(1)
    val col2 = args(2)
    val mink = args(3).toInt
    val maxDegree: Int = args(4).toInt
    val partition: Int = args(5).toInt
    val saveMode = args(6)
    val saveArg = args(7)

    try {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName("mce")
        .setMaster("yarn")
      val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
      val startTime = System.currentTimeMillis()
      val sql = s"select * from ${tableName}"

      val edges: DataFrame = spark.sql(sql).select(col1, col2)
      val edgesRDD: RDD[(String, String)] = edges.rdd.map(row => (row(0).toString, row(1).toString))

      val result: (RDD[(Int, String)], RDD[(Int, String)]) = MaximalCliqueEnumeration.run(edgesRDD, mink, maxDegree, partition)
      val _ = saveMode match {
        case "hive" => {
          val nodeMap: RDD[Row] = result._1.asInstanceOf[RDD[(Int, String)]].map(t => Row(t._2, t._1))
          val cliqueMap: RDD[Row] = result._2.asInstanceOf[RDD[(Int, String)]].map(t => Row(t._1, t._2))
          val schema_nodeMap: StructType = StructType(List(StructField("_id", StringType, true), StructField("_id_int", IntegerType, true)))
          val schema_cliqueMap: StructType = StructType(List(StructField("_id_int", IntegerType, true), StructField("_clique_id", StringType, true)))

          val nodeMapDF: DataFrame = spark.createDataFrame(nodeMap, schema_nodeMap)
          val cliqueMapDF: DataFrame = spark.createDataFrame(cliqueMap, schema_cliqueMap)
          nodeMapDF.createOrReplaceTempView("nodeMapTmpV")
          cliqueMapDF.createOrReplaceTempView("cliqueMapTmpV")

          val outputTableName: String = saveArg
          val createSql = s"create table ${outputTableName}_mce_nodeMap(_id varchar(250), _id_int int)"
          val createSql2 = s"create table ${outputTableName}_mce_cliqueMap(_id_int int,_clique_id varchar(20))"
          spark.sql(createSql)
          spark.sql(createSql2)

          spark.sql(s"insert into ${outputTableName}_mce_nodeMap select * from nodeMapTmpV")
          spark.sql(s"insert into ${outputTableName}_mce_cliqueMap select * from cliqueMapTmpV")
        }

        case "hdfs" => {
          val outputPath = saveArg
          val mapInfoPath = s"${outputPath}/${tableName}/map_info"
          val cliquePath = s"${outputPath}/${tableName}/clique_info"

          Util.saveDataToHDFS(result._1, MCE_OUTPUT_SPLIT, mapInfoPath)
          Util.saveDataToHDFS(result._2, MCE_OUTPUT_SPLIT, cliquePath)

        }
        case _ => throw new Exception("illegal save mode")
      }


      val costTime: Double = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Exec Successful:maximal clique detection costTime:${costTime}s")
      spark.close()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure:${e.getMessage}")
        throw e
    }
  }

}
