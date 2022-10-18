package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.MatrixVerify

import org.apache.spark.ml.stat
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Matrix, Vectors, Vector}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.stat.{Statistics => OldStatistics}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
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

class PearsonConfig extends Serializable {
  @BeanProperty var pearson: util.HashMap[String, util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]]] = _
}

class PearsonParams extends Serializable {
  @BeanProperty var pt: Int = _

  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object PearsonRunner {

  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (dataStructure, datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))
      val trainingDataPath = args(1)
      val cpuName = args(2)
      val sparkConfSplit = args(3).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))
      val saveResultPath = args(4)

      val stream = Utils.getStream("conf/ml/pearson/pearson.yml")
      val representer = new Representer
      representer.addClassTag(classOf[PearsonParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[PearsonConfig]), representer, options)
      val description = new TypeDescription(classOf[PearsonParams])
      yaml.addTypeDescription(description)
      val configs: PearsonConfig = yaml.load(stream).asInstanceOf[PearsonConfig]
      val paramsMap: util.HashMap[String, Object] = configs.pearson.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(dataStructure).get(datasetName)
      val params = new PearsonParams()
      params.setPt(paramsMap.getOrDefault("pt", "1000").asInstanceOf[Int])
      params.setTrainingDataPath(trainingDataPath)
      params.setApiName(dataStructure)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("Pearson")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${dataStructure}_${datasetName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${dataStructure}_${datasetName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${dataStructure}_${datasetName}_raw"
        params.setVerifiedDataPath(params.saveDataPath)
        params.setSaveDataPath(s"${params.saveDataPath}_raw")
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName).setMaster(master)
      val commonParas = Array (
        ("spark.submit.deployMode", deployMode),
        ("spark.executor.instances", numExec),
        ("spark.executor.cores", execCores),
        ("spark.executor.memory", execMem)
      )
      conf.setAll(commonParas)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      println(s"Initialized spark session.")

      val costTime = dataStructure match {
        case "dataframe" =>
          new PearsonKernel().runDataframeJob(spark, params)
        case "rdd" =>
          new PearsonKernel().runRddJob(spark, params)
      }
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
      println(s"Exec Successful: costTime: ${costTime}s;isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

class PearsonKernel {

  def runDataframeJob(spark: SparkSession, params: PearsonParams): Double = {
    val pt = params.pt
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val startTime = System.currentTimeMillis()
    val data = spark.createDataFrame(
      sc
        .textFile(params.trainingDataPath)
        .map(x=>Row(Vectors.dense(x.split(",").map(_.toDouble))))
        .repartition(pt),
      StructType(List(StructField("matrix", VectorType)))
    ).persist(StorageLevel.MEMORY_ONLY)

    val result = stat.Correlation.corr(data, "matrix")
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    val mat = result.collect()(0).getAs[Matrix](0)
    val pearsonMat = new DenseMatrix(mat.numRows, mat.numCols, mat.toArray, mat.isTransposed)
    //save result
    MatrixVerify.saveMatrix(pearsonMat, params.saveDataPath, sc)
    costTime
  }

  def runRddJob(spark: SparkSession, params: PearsonParams): Double = {

    val pt = params.pt
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()
    val data = spark.createDataFrame(
      sc.textFile(params.trainingDataPath)
        .map(x=>Row(Vectors.dense(x.split(",").map(_.toDouble))))
        .repartition(pt),
      StructType(List(StructField("matrix", VectorType)))
    ).persist(StorageLevel.MEMORY_ONLY)

    val rdd = data.select("matrix").rdd.map{
      case Row(v: Vector) => OldVectors.fromML(v)
    }

    val oldM = OldStatistics.corr(rdd, "pearson")
    val pearsonMat = oldM.asInstanceOf[DenseMatrix]
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    //save result
    MatrixVerify.saveMatrix(pearsonMat, params.saveDataPath, sc)

    costTime
  }

}
