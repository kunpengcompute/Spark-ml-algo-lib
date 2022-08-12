package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.UpEvaluationVerify

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.clustering.{LDAModel, LDA => MLLDA}
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector => MLlibVector}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.clustering.{LocalLDAModel, LDA => MLlibLDA}
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter}
import java.util.HashMap
import java.util
import scala.beans.BeanProperty


class LDAConfig extends Serializable {

  @BeanProperty var lda: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class LDAParams extends Serializable {

  @BeanProperty var numPartitions: Int = _
  @BeanProperty var numPartitionsTest: Int = _
  @BeanProperty var numFeatures: Int = _
  @BeanProperty var checkpointInterval: Int = _
  @BeanProperty var inputDataType: String = _
  @BeanProperty var maxIter: Int = _
  @BeanProperty var k: Int = _

  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var dataStructure: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var evaluation: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object LDARunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (dataStructure, datasetName, apiName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3), modelConfSplit(4))
      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/lda/lda.yml")
      val representer = new Representer
      representer.addClassTag(classOf[LDAParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[LDAConfig]), representer, options)
      val description = new TypeDescription(classOf[LDAParams])
      yaml.addTypeDescription(description)
      val config: LDAConfig = yaml.load(stream).asInstanceOf[LDAConfig]
      val paramsMap: util.HashMap[String, Object] = config.lda.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new LDAParams()
      params.setNumPartitions(paramsMap.get("numPartitions").asInstanceOf[Int])
      params.setNumPartitionsTest(paramsMap.get("numPartitionsTest").asInstanceOf[Int])
      params.setNumFeatures(paramsMap.get("numFeatures").asInstanceOf[Int])
      params.setCheckpointInterval(paramsMap.get("checkpointInterval").asInstanceOf[Int])
      params.setInputDataType(paramsMap.get("inputDataType").asInstanceOf[String])
      params.setMaxIter(paramsMap.get("maxIter").asInstanceOf[Int])
      params.setK(paramsMap.get("k").asInstanceOf[Int])
      params.setApiName(apiName)
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setDataStructure(dataStructure)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("LDA")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}_${dataStructure}_${apiName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${dataStructure}_${datasetName}_${apiName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${dataStructure}_${datasetName}_${apiName}_raw"
        params.setVerifiedDataPath(params.saveDataPath)
        params.setSaveDataPath(s"${params.saveDataPath}_raw")
      }
      params.setTestcaseType(appName)
      val conf = new SparkConf().setAppName(appName)
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val (res, costTime) = dataStructure match {
        case "dataframe" => {
          val (trainModel, dataFrameCostTime) = new LDAKernel().runDataFrameJob(spark, params)
          val confPredict = new SparkConf()
            .setAppName(s"LDA_${dataStructure}_${datasetName}_${apiName}_${cpuName}_predict").set("spark.task.cpus", "1")
          //val sparkPredict = SparkSession.builder.config(confPredict).getOrCreate()
          val dataFrameRes = new LDAKernel().runPredictJob(spark, params, trainModel)
          (dataFrameRes, dataFrameCostTime)
        }
        case "rdd" => (0.0, new LDAKernel().runRDDJob(spark, params))
      }
      params.setEvaluation(res)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(UpEvaluationVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
        val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
        writerIsCorrect.write(s"${params.testcaseType} ${params.isCorrect} \n")
        writerIsCorrect.close()
      }

      val writer = new FileWriter(s"report/${params.testcaseType}_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s; evaluation: ${res};isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}


class LDAKernel {

  def runDataFrameJob(spark: SparkSession, params: LDAParams): (LDAModel, Double) = {
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()
    val trainingData = if (params.inputDataType == "libsvm") {
      spark.read.format("libsvm")
        .option("numFeatures", params.numFeatures)
        .load(params.trainingDataPath)
        .repartition(params.numPartitions)
        .persist()
    } else {
      import spark.implicits._
      val corpus: RDD[(Long, MLlibVector)] = sc.objectFile(params.trainingDataPath)
      corpus
        .map(i => (i._1, i._2.asML))
        .toDF()
        .repartition(params.numPartitions)
        .withColumnRenamed("_1", "label")
        .withColumnRenamed("_2", "features")
        .persist()
    }

    println("count: " + trainingData.count())
    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)
    val lda = new MLLDA()
      .setSeed(2020L)
      .setK(params.k)
      .setMaxIter(params.maxIter)
      .setCheckpointInterval(params.checkpointInterval)

    val paramMap = ParamMap(lda.k -> params.k)
      .put(lda.maxIter, params.maxIter)

    val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
    for (i <- 0 to paramMaps.size - 1) {
      paramMaps(i) = ParamMap(lda.k -> params.k)
        .put(lda.maxIter, params.maxIter)
    }
    val maxIterParamPair = ParamPair(lda.maxIter, params.maxIter)
    val kPair = ParamPair(lda.k, params.k)
    val model = params.apiName match {
      case "fit" => lda.fit(trainingData)
      case "fit1" => lda.fit(trainingData, paramMap)
      case "fit2" =>
        val models = lda.fit(trainingData, paramMaps)
        models(0)
      case "fit3" => lda.fit(trainingData, kPair, maxIterParamPair)
    }
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    println("load and train costTime: " + costTime)

    trainingData.unpersist()
    //spark.close()
    (model, costTime)
  }

  def runPredictJob(spark: SparkSession, params: LDAParams, model: LDAModel): Double = {
    val sc = spark.sparkContext

    println("====== Start Predict =======: ")
    val startTime = System.currentTimeMillis()
    val testData = if (params.inputDataType == "libsvm") {
      spark.read.format("libsvm")
        .option("numFeatures", params.numFeatures)
        .load(params.testDataPath)
        .repartition(params.numPartitionsTest)
        .persist()
    } else {
      import spark.implicits._
      val corpus: RDD[(Long, MLlibVector)] = sc.objectFile(params.testDataPath)
      corpus
        .map(i => (i._1, i._2.asML))
        .toDF()
        .repartition(params.numPartitionsTest)
        .withColumnRenamed("_1", "label")
        .withColumnRenamed("_2", "features")
        .persist()
    }
    val testDataSize = testData.count()
    val fraction = if (testDataSize <= 10000) {
      1.0
    } else {
      10000.0 / testDataSize.toDouble
    }
    val testDataSample = testData.sample(fraction, 2020L)

    val res = model.logLikelihood(testDataSample)
    println("====== End Predict =======: ")
    val predictTime = (System.currentTimeMillis() - startTime) / 1000.0
    println("Predict costTime: " + (predictTime))

    testData.unpersist()
    var result = new java.math.BigDecimal(res)
    Utils.saveLDARes(result, params.saveDataPath, sc)
    res
  }

  def runRDDJob(spark: SparkSession, params: LDAParams): Double = {
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()
    val trainingData = if (params.inputDataType == "libsvm") {
      MLUtils.loadLibSVMFile(sc, params.trainingDataPath, params.numFeatures)
        .map(i => (i.label.toLong, i.features))
        .repartition(params.numPartitions)
        .persist()
    } else {
      val corpus: RDD[(Long, MLlibVector)] = sc.objectFile(params.trainingDataPath)
      corpus
        .repartition(params.numPartitions)
        .persist()
    }
    println("count: " + trainingData.count())
    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)

    val model = new MLlibLDA()
      .setSeed(2020L)
      .setK(params.k)
      .setMaxIterations(params.maxIter)
      .setCheckpointInterval(params.checkpointInterval)
      .setOptimizer("online")
      .run(trainingData)

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    trainingData.unpersist()
    Utils.saveEvaluation(0.0, params.saveDataPath, sc)
    costTime
  }
}
