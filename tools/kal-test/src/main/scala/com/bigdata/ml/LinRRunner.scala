package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.DownEvaluationVerify

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions.udf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter}
import java.util
import scala.beans.BeanProperty

class LinRConfig extends Serializable {

  @BeanProperty var linR: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class LinRParams extends Serializable {

  @BeanProperty var pt: Int = _
  @BeanProperty var numFeatures: Int = _
  @BeanProperty var loss: String = _
  @BeanProperty var regParam: Double = _
  @BeanProperty var elasticNetParam: Double = _
  @BeanProperty var maxIter: Int = _
  @BeanProperty var tolerance: Double = _

  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var evaluation: Double = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object LinRRunner {
  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, apiName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))
      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))
      val cpuName = args(2)
      val sparkConfSplit = args(3).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))
      val saveResultPath = args(4)

      val stream = Utils.getStream("conf/ml/linR/linR.yml")
      val representer = new Representer
      representer.addClassTag(classOf[LinRParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[LinRConfig]), representer, options)
      val description = new TypeDescription(classOf[LinRParams])
      yaml.addTypeDescription(description)
      val configs: LinRConfig = yaml.load(stream).asInstanceOf[LinRConfig]
      val paramsMap: util.HashMap[String, Object] = configs.linR.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new LinRParams()
      params.setPt(paramsMap.getOrDefault("pt", "2000").asInstanceOf[Int])
      params.setNumFeatures(paramsMap.getOrDefault("numFeatures", "500").asInstanceOf[Int])
      params.setLoss(paramsMap.getOrDefault("loss", "squaredError").asInstanceOf[String])
      params.setRegParam(paramsMap.getOrDefault("regParam", "0.01").asInstanceOf[Double])
      params.setElasticNetParam(paramsMap.getOrDefault("elasticNetParam", "0.0").asInstanceOf[Double])
      params.setMaxIter(paramsMap.getOrDefault("maxIter", "500").asInstanceOf[Int])
      params.setTolerance(paramsMap.getOrDefault("tolerance", "1E-6").asInstanceOf[Double])
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setApiName(apiName)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("LinR")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${datasetName}_${apiName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${datasetName}_${apiName}_raw"
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

      val (res, costTime) = new LinRKernel().runJob(spark, params)
      params.setEvaluation(res)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(DownEvaluationVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
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

class LinRKernel {

  def runJob(spark: SparkSession, params: LinRParams): (Double, Double) = {

    import spark.implicits._
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()
    val trainData = spark
      .read
      .format("libsvm")
      .option("vectorType", "dense")
      .option("numFeatures", params.numFeatures)
      .load(params.trainingDataPath)
      .repartition(params.pt)
      .cache()

    val trainingData = if (params.datasetName == "rcv") {
      val label_convert = udf((x: Double) => if (x < 0.0) 0.0 else 1.0)
      trainData.withColumn("label", label_convert($"label"))
    } else {
      trainData
    }
    println(s"trainingData: ${trainingData.count()}")
    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)

    val numIterationJY = 10
    val regParamJY = 0.01

    val linR = new LinearRegression()
      .setSolver("l-bfgs")
      .setLoss(params.loss)
      .setRegParam(regParamJY)
      .setElasticNetParam(params.elasticNetParam)
      .setMaxIter(numIterationJY)
      .setTol(params.tolerance)

    if (params.apiName == "fit"){
      linR.setRegParam(params.regParam)
      linR.setMaxIter(params.maxIter)
    }

    val paramMap = ParamMap(linR.maxIter -> params.maxIter)
      .put(linR.regParam, params.regParam)
    val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
    for (i <- 0 to paramMaps.size -1) {
      paramMaps(i) = ParamMap(linR.maxIter -> params.maxIter)
        .put(linR.regParam, params.regParam)
    }
    val maxIterParamPair = ParamPair(linR.maxIter, params.maxIter)
    val regParamPair = ParamPair(linR.regParam, params.regParam)

    val model = params.apiName match {
      case "fit" => linR.fit(trainingData)
      case "fit1" => linR.fit(trainingData, paramMap)
      case "fit2" =>
        val models = linR.fit(trainingData, paramMaps)
        models(0)
      case "fit3" => linR.fit(trainingData, maxIterParamPair, regParamPair)
    }
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)

    val testData = spark
      .read
      .format("libsvm")
      .option("vectorType", "dense")
      .option("numFeatures", params.numFeatures)
      .load(params.testDataPath)
      .repartition(params.pt)
      .cache()

    val testingData = if (params.datasetName == "rcv") {
      val label_convert = udf((x: Double) => if (x < 0.0) 0.0 else 1.0)
      testData.withColumn("label", label_convert($"label"))
    } else {
      testData
    }

    val getSquaredError = udf((v1: Double, v2: Double) => {
      math.pow((v1 - v2), 2)
    })
    val predictions = model.transform(testingData)
      .withColumn("squaredError", getSquaredError($"label", $"prediction"))
      .select("squaredError").summary("mean")

    val res = predictions.select("squaredError").first().getString(0).toDouble
    Utils.saveEvaluation(res, params.saveDataPath, sc)
    (res, costTime)
  }
}