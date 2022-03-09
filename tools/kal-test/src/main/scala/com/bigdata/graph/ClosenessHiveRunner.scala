package com.bigdata.graph

import org.apache.spark.graphx.lib.Closeness
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf

object ClosenessHiveRunner {
  def main(args: Array[String]): Unit = {
    if (args.length < 10) {
      println(args.mkString(","))
      println("Usage:KCoreDecompositionRunner <tableName> <col1> <col2> <colWeight> <weighted> <k> <p> <partition> <save_mode> <save_arg>")
      System.exit(-1)
    }
    val tableName: String = args(0)
    val col1: String = args(1)
    val col2: String = args(2)
    val colWeight: String = args(3)
    val weighted: Boolean = args(4).toBoolean
    val k: Int = args(5).toInt
    val p: Double = args(6).toDouble
    val partition: Int = args(7).toInt
    val saveMode: String = args(8)
    val saveArg: String = args(9)

    try {
      val appName = s"Closeness_${tableName}_${weighted}"
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
        val edges: DataFrame = spark.sql(sql).select(col1, col2)
        val tmpRDD1: RDD[(String, String)] = edges.rdd.map(row => (row(0).toString, row(1).toString))
        verticesRDD = tmpRDD1.flatMap(f => Iterator(f._1, f._2)).distinct.zipWithIndex.cache
        verticesRDD.foreachPartition(_ => {})
        println("vertices count:" +verticesRDD.count())
        edgesRDD = tmpRDD1.leftOuterJoin(verticesRDD, partition).map(f => (f._2._1, f._2._2.get)).leftOuterJoin(verticesRDD, partition).map(f => (f._2._1, f._2._2.get, 1.0))
      } else {
        val edges: DataFrame = spark.sql(sql).select(col1, col2, colWeight)
        val tmpRDD: RDD[(String, (String, Double))] = edges.rdd.map(row => (row(0).toString, (row(1).toString, row(2).toString.toDouble)))
        verticesRDD = tmpRDD.flatMap(f => Iterator(f._1, f._2._1)).distinct().zipWithIndex().cache()
        verticesRDD.foreachPartition(_ => {})
        println("vertices count:" + verticesRDD.count())
        edgesRDD = tmpRDD.leftOuterJoin(verticesRDD, partition).map(f => (f._2._1._1, (f._2._2.get, f._2._1._2))).leftOuterJoin(verticesRDD, partition).map(f => (f._2._1._1, f._2._2.get, f._2._1._2))
      }

      val result: RDD[(Long, Double)] =
        Closeness.run(edgesRDD, weighted, k, p)
      val finalResult = result.leftOuterJoin(verticesRDD.map(_.swap), 200).map(f => Row(f._2._2.get, f._2._1))

      val _ = saveMode match {
        case "hive" => {
          val schema_resultMap: StructType = StructType(List(StructField("_node_id", StringType, true), StructField("_closeness", DoubleType, true)))
          val resultMapDF: DataFrame = spark.createDataFrame(finalResult, schema_resultMap)
          resultMapDF.createOrReplaceTempView("ClosenessMapTmpV")
          val outputTableName: String = saveArg
          val createSql = s"create table ${outputTableName}_closeness(_node_id varchar(250), _closeness double)"
          spark.sql(createSql)
          spark.sql(s"insert into ${outputTableName}_closeness select * from ClosenessMapTmpV")
        }
        case "hdfs" => {
          val outputPath: String = saveArg
          Util.saveDataToHDFS(result, ",", outputPath)
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
