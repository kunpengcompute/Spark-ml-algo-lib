package com.bigdata.ml

import com.bigdata.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.ml
import org.apache.spark.ml.linalg.{Matrix, Vectors, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter}
import java.util
import scala.beans.BeanProperty

class SpearManConfig extends Serializable {

  @BeanProperty var spearman: util.HashMap[String, Object] = _
}

class SpearManParams extends Serializable {
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var datasetCpuName: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var costTime: Double = _
  @BeanProperty var resultSum: Double = _
  @BeanProperty var resultAvg: Double = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

class SpearManKernel {
  def runDataframeJob(spark: SparkSession,params: SpearManParams): (Double, Double, Double) = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val startTime = System.currentTimeMillis()

    val data = spark.createDataFrame(
      spark
        .sparkContext
        .textFile(params.dataPath)
        .map(x=>Row(Vectors.dense(x.split(",").map(_.toDouble))))
        .repartition(params.numPartitions),
       StructType(List(StructField("matrix", VectorType)))
    ).persist(StorageLevel.MEMORY_ONLY)

    val mat_df = ml.stat.Correlation.corr(data,"matrix", method = "spearman")

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val result = mat_df.collect()(0).getAs[Matrix](0).toArray
    val result_avg = result.sum/result.length

    val saveFile = new Path(s"${params.saveDataPath}/${params.apiName}")
    val saveFileRaw = new Path(s"${params.saveDataPath}/raw/${params.apiName}")
    if (fs.exists(saveFile) || fs.exists(saveFileRaw)) {
      fs.delete(saveFile, true)
      fs.delete(saveFileRaw, true)
    }

    val res_new = mat_df.collect()(0).getAs[Matrix](0)
    val res_new2 = res_new.toArray
    val res_new3 = new Array[String](res_new.numCols)

    for(i <- 0 until res_new.numRows) {
      val row = res_new2.slice(i * res_new.numRows, (i + 1) * res_new.numRows).mkString(";")
      res_new3(i) = i + " " + row
    }
    if(params.isRaw == "yes"){
      sc.parallelize(res_new3).repartition(100).saveAsTextFile(s"${params.saveDataPath}/raw/${params.apiName}")
    }
    else {
      sc.parallelize(res_new3).repartition(100).saveAsTextFile(s"${params.saveDataPath}/${params.apiName}")
    }

    (result.sum,result_avg, costTime)
  }

  def runRddJob(spark: SparkSession,params: SpearManParams): (Double, Double, Double) = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val startTime = System.currentTimeMillis()

    val data = spark.createDataFrame(
      spark
        .sparkContext
        .textFile(params.dataPath)
        .map(x=>Row(Vectors.dense(x.split(",").map(_.toDouble))))
        .repartition(params.numPartitions),
      StructType(List(StructField("matrix", VectorType)))
    ).persist(StorageLevel.MEMORY_ONLY)

    val rdd = data.select("matrix").rdd.map{
      case Row(v: Vector) => OldVectors.fromML(v)
    }
    val mat_rdd = mllib.stat.Statistics.corr(rdd,method = "spearman")

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val result = mat_rdd.toArray
    val result_avg = result.sum/result.length

    val saveFile = new Path(s"${params.saveDataPath}/${params.apiName}")
    val saveFileRaw = new Path(s"${params.saveDataPath}/raw/${params.apiName}")
    if (fs.exists(saveFile) || fs.exists(saveFileRaw)) {
      fs.delete(saveFile, true)
      fs.delete(saveFileRaw, true)
    }
    val res = new Array[String](mat_rdd.numRows)
    for(i <- 0 until mat_rdd.numRows) {
      val row = result.slice(i * mat_rdd.numRows, (i + 1) * mat_rdd.numCols).mkString(";")
      res(i) = i.toString + " " + row
    }
    if(params.isRaw == "yes"){
      sc.parallelize(res).repartition(100).saveAsTextFile(s"${params.saveDataPath}/raw/${params.apiName}")
    }
    else {
      sc.parallelize(res).repartition(100).saveAsTextFile(s"${params.saveDataPath}/${params.apiName}")
    }

    (result.sum,result_avg,costTime)
  }
}



object SpearManRunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (dataStructure, datasetName, platformName) = (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))

      val dataAllPath = args(1)
      val dataPathSplit = dataAllPath.split(",")
      val (dataPath, saveDataPath) = (dataPathSplit(0), dataPathSplit(1))

      val datasetCpuName = s"${datasetName}_${platformName}"
      val stream = Utils.getStream("conf/ml/spearman/spearman.yml")

      val representer = new Representer
      representer.addClassTag(classOf[SpearManParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SpearManConfig]), representer, options)
      val description = new TypeDescription(classOf[SpearManParams])
      yaml.addTypeDescription(description)
      val config: SpearManConfig = yaml.load(stream).asInstanceOf[SpearManConfig]
      val paramsMap = config.spearman.get(datasetCpuName).asInstanceOf[util.HashMap[String, Object]]

      val params = new SpearManParams()

      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setDatasetCpuName(datasetCpuName)
      params.setApiName(dataStructure)
      params.setSaveDataPath(saveDataPath)
      params.setAlgorithmName("SpearMan")
      params.setTestcaseType(s"SpearMan_${dataStructure}_${datasetName}")

      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)

      val conf = new SparkConf().setAppName(s"SpearMan_${datasetName}_${dataStructure}_${platformName}")

      if (platformName == "raw") {
        params.setIsRaw("yes")
      }
      val spark = SparkSession.builder.config(conf).getOrCreate()


      val tuple = dataStructure match {
        case "dataframe" =>
          new SpearManKernel().runDataframeJob(spark, params)
        case "rdd" =>
          new SpearManKernel().runRddJob(spark, params)
      }

      params.setResultSum(tuple._1)
      params.setResultAvg(tuple._2)
      params.setCostTime(tuple._3)
      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/SpearMan${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${params.getCostTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}




