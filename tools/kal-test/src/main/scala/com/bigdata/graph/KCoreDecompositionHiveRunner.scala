package com.bigdata.graph

import org.apache.spark.graphx.lib.KCoreDecomposition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf

object KCoreDecompositionHiveRunner {
  private val RESULT_SPLIT = ","

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      println(args.mkString(","))
      println("Usage:KCoreDecompositionRunner <table name> <col1> <col2> <partition> <save_mode> <save_arg>")
      System.exit(-1)
    }
    val tableName: String = args(0)
    val col1: String = args(1)
    val col2: String = args(2)
    val partition: Int = args(3).toInt
    val saveMode: String = args(4)
    val saveArg: String = args(5)

    val appName = s"KCORE_${tableName}"

    try {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(appName)
        .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

      val startTime: Long = System.currentTimeMillis()
      val sql = s"select * from ${tableName}"
      val edges: DataFrame = spark.sql(sql).select(col1, col2)
      // string to long
      val tmpRDD: RDD[(String, String)] = edges.rdd.map(row => (row(0).toString, row(1).toString))
      val verticesRDD: RDD[(String, Long)] = tmpRDD.flatMap(f => Iterator(f._1, f._2)).distinct.zipWithIndex.cache
      verticesRDD.foreachPartition(_ => {})
      println("vertices count:" + verticesRDD.count())
      val indexEdgeRDD: RDD[(Long, Long)] = tmpRDD.leftOuterJoin(verticesRDD, partition).map(f => (f._2._1, f._2._2.get)).leftOuterJoin(verticesRDD, partition).map(f => (f._2._1, f._2._2.get))

      val result: RDD[(Long, Int)] = KCoreDecomposition.run(indexEdgeRDD)
      println("result count: " + result.count())
      val finalResult: RDD[Row] = result.leftOuterJoin(verticesRDD.map(_.swap), partition).map(f => Row(f._2._2.get, f._2._1))
      val _ = saveMode match {
        case "hive" => {
          val schema_resultMap: StructType = StructType(List(StructField("_node_id", StringType, true), StructField("_coreness", IntegerType, true)))
          val resultMapDF: DataFrame = spark.createDataFrame(finalResult, schema_resultMap)
          resultMapDF.createOrReplaceTempView("coreMapTmpV")
          val outputTableName: String = saveArg
          val createSql = s"create table ${outputTableName}_kcore(_node_id varchar(250), _coreness int)"
          spark.sql(createSql)
          spark.sql(s"insert into ${outputTableName}_kcore select * from coreMapTmpV")
        }
        case "hdfs" => {
          val outputPath: String = saveArg
          Util.saveDataToHDFS(result, RESULT_SPLIT, outputPath)
        }
        case _ => throw new Exception("illegal save mode")
      }

      val costTime: Double = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Exec Successful: KCore Decomposition costTime: ${costTime}s")
      spark.close()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure:${e.getMessage}")
        throw e
    }
  }
}
