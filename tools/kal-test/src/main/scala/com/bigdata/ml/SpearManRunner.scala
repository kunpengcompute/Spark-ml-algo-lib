package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.MatrixVerify

import org.apache.spark.SparkConf
import org.apache.spark.ml
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Matrix, Vectors, Vector}
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter}
import java.util
import scala.beans.BeanProperty

class SpearManConfig extends Serializable {

  @BeanProperty var spearman: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class SpearManParams extends Serializable {

  @BeanProperty var numPartitions: Int = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var dataStructure: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var resultSum: Double = _
  @BeanProperty var resultAvg: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object SpearManRunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (dataStructure, datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/spearman/spearman.yml")
      val representer = new Representer
      representer.addClassTag(classOf[SpearManParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SpearManConfig]), representer, options)
      val description = new TypeDescription(classOf[SpearManParams])
      yaml.addTypeDescription(description)
      val config: SpearManConfig = yaml.load(stream).asInstanceOf[SpearManConfig]
      val paramsMap: util.HashMap[String, Object] = config.spearman.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new SpearManParams()
      params.setNumPartitions(paramsMap.get("numPartitions").asInstanceOf[Int])
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setDataStructure(dataStructure)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("SpearMan")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}_${dataStructure}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${dataStructure}_${datasetName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${dataStructure}_${datasetName}_raw"
        params.setVerifiedDataPath(params.saveDataPath)
        params.setSaveDataPath(s"${params.saveDataPath}_raw")
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val (resultSum,resultAvg,costTime) = dataStructure match {
        case "dataframe" =>
          new SpearManKernel().runDataframeJob(spark, params)
        case "rdd" =>
          new SpearManKernel().runRddJob(spark, params)
      }
      params.setResultSum(resultSum)
      params.setResultAvg(resultAvg)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(MatrixVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
        val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
        writerIsCorrect.write(s"${params.testcaseType} ${params.isCorrect} \n")
        writerIsCorrect.close()
      }

      val writer = new FileWriter(s"report/${params.testcaseType}_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${params.getCostTime}s;isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
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

    val mat = mat_df.collect()(0).getAs[Matrix](0)
    val result = mat.toArray
    val result_avg = result.sum/result.length
    val spearManMat = new DenseMatrix(mat.numRows, mat.numCols, mat.toArray, mat.isTransposed)


    MatrixVerify.saveMatrix(spearManMat, params.saveDataPath, sc)
    (result.sum, result_avg, costTime)
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
    val mat_rdd = mllib.stat.Statistics.corr(rdd, method = "spearman")
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val result = mat_rdd.toArray
    val result_avg = result.sum/result.length
    val spearManMat = mat_rdd.asInstanceOf[DenseMatrix]

    MatrixVerify.saveMatrix(spearManMat, params.saveDataPath, sc)
    (result.sum, result_avg, costTime)
  }
}




