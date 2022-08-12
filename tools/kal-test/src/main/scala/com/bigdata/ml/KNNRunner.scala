package com.bigdata.ml

import com.bigdata.utils.Utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.neighbors.KNN
import org.apache.spark.ml.linalg.Vectors
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter, PrintWriter}
import java.util
import scala.beans.BeanProperty

class KNNConfig extends Serializable {

  @BeanProperty var knn: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class KNNParams extends Serializable {

  @BeanProperty var pt: Int = _
  @BeanProperty var k: Int = _
  @BeanProperty var testNum: Int = _
  @BeanProperty var testBatchSize: Int = _
  @BeanProperty var featuresCol: String = _
  @BeanProperty var distanceCol: String = _
  @BeanProperty var neighborsCol: String = _
  @BeanProperty var topTreeSizeRate: Double = _
  @BeanProperty var topTreeLeafSize: Int = _
  @BeanProperty var subTreeLeafSize: Int = _

  @BeanProperty var inputDataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
}

object KNNRunner {
  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw) =
        (modelConfSplit(0), modelConfSplit(1))
      val inputDataPath = args(1)
      val cpuName = args(2)
      val sparkConfSplit = args(3).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))
      val saveResultPath = args(4)
      
      val stream = Utils.getStream("conf/ml/knn/knn.yml")
      val representer = new Representer
      representer.addClassTag(classOf[KNNParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[KNNConfig]), representer, options)
      val description = new TypeDescription(classOf[KNNParams])
      yaml.addTypeDescription(description)
      val configs: KNNConfig = yaml.load(stream).asInstanceOf[KNNConfig]
      val params = new KNNParams()
      val paramsMap: util.HashMap[String, Object] = configs.knn.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      params.setPt(paramsMap.getOrDefault("pt", "200").asInstanceOf[Int])
      params.setK(paramsMap.getOrDefault("k", "10").asInstanceOf[Int])
      params.setTestNum(paramsMap.getOrDefault("testNum", "100000").asInstanceOf[Int])
      params.setTestBatchSize(paramsMap.getOrDefault("testBatchSize", "10").asInstanceOf[Int])
      params.setTopTreeSizeRate(paramsMap.getOrDefault("topTreeSizeRate", "10.0").asInstanceOf[Double])
      params.setTopTreeLeafSize(paramsMap.getOrDefault("topTreeLeafSize", "10").asInstanceOf[Int])
      params.setSubTreeLeafSize(paramsMap.getOrDefault("subTreeLeafSize", "30").asInstanceOf[Int])
      params.setFeaturesCol("features")
      params.setDistanceCol("distances")
      params.setNeighborsCol("neighbors")
      params.setInputDataPath(inputDataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("KNN")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${datasetName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${datasetName}_raw"
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

      val costTime = if (isRaw == "no") {
        new KNNKernel().runJob(spark, params)
      } else {
        new KNNKernel().runRawJob(spark, params)
      }
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(s"report/${params.testcaseType}_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

class KNNKernel {

  def runJob(spark: SparkSession, params: KNNParams): Double = {

    import spark.implicits._
    val startTime = System.currentTimeMillis()
    val dataPath = params.inputDataPath
    val featuresCol = params.featuresCol
    val testNum = params.testNum
    val pt = params.pt
    val neighborsCol = params.neighborsCol
    val distanceCol = params.distanceCol
    val testBatchSize = params.testBatchSize
    val k = params.k

    //read data
    val rawData = spark.sparkContext.textFile(dataPath)
      .map(line => {
        val arr = line.split("\t")
        val id = arr(0).toLong
        val feature = Vectors.dense(arr(1).split(",").map(_.toDouble))
        (id, feature)
      }).toDF("id", featuresCol).cache()

    //split train/test datasets
    val trainDataDF = rawData.filter($"id" >= testNum).repartition(pt).cache()
    val testDataDF = rawData.filter($"id" < testNum).repartition(pt).cache()
    trainDataDF.count()
    testDataDF.count()

    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)

    //fit
    val model = new KNN()
      .setFeaturesCol(featuresCol)
      .setAuxiliaryCols(Array("id"))
      .fit(trainDataDF)

    //transform
    val testResults = model
        .setNeighborsCol(neighborsCol)
        .setDistanceCol(distanceCol)
        .setK(k)
        .setTestBatchSize(testBatchSize)
        .transform(testDataDF).cache()
    testResults.count()

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    costTime
  }

  def runRawJob(spark: SparkSession, params: KNNParams): Double = {

    import spark.implicits._
    val startTime = System.currentTimeMillis()
    val dataPath = params.inputDataPath
    val featuresCol = params.featuresCol
    val testNum = params.testNum
    val pt = params.pt
    val neighborsCol = params.neighborsCol
    val distanceCol = params.distanceCol
    val k = params.k
    val topTreeSizeRate = params.topTreeSizeRate
    val topTreeLeafSize = params.topTreeLeafSize
    val subTreeLeafSize = params.subTreeLeafSize

    //read data
    val rawData = spark.sparkContext.textFile(dataPath)
      .map(line => {
        val arr = line.split("\t")
        val id = arr(0).toLong
        val feature = Vectors.dense(arr(1).split(",").map(_.toDouble))
        (id, feature)
      }).toDF("id", featuresCol).cache()

    //split train/test datasets
    val trainDataDF = rawData.filter($"id" >= testNum).repartition(pt).cache()
    val testDataDF = rawData.filter($"id" < testNum).repartition(pt).cache()
    trainDataDF.count()
    testDataDF.count()

    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)

    //fit
    import org.apache.spark.ml.knn.KNN
    val model = new KNN()
      .setTopTreeSize((pt * topTreeSizeRate).toInt)
      .setTopTreeLeafSize(topTreeLeafSize)
      .setSubTreeLeafSize(subTreeLeafSize)
      .setBalanceThreshold(0.0)
      .setFeaturesCol(featuresCol)
      .setAuxCols(Array("id"))
      .fit(trainDataDF)

    //transform
    val testResults = model
      .setBufferSize(Double.MaxValue)
      .setNeighborsCol(neighborsCol)
      .setDistanceCol(distanceCol)
      .setK(k)
      .transform(testDataDF).cache()
    testResults.count()

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    costTime
  }

}
