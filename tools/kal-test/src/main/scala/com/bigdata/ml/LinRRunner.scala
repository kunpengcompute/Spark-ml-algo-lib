package com.bigdata.ml

import java.io.{File, FileWriter}
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions.udf
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

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
}

object LinRRunner {
  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("_")
      val (datasetName, apiName) =
        (modelConfSplit(0), modelConfSplit(1))

      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))

      val cpuName = args(2)
      val isRaw = args(3)
      val sparkConfSplit = args(4).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))

      val stream = (cpuName, isRaw) match {
        case ("aarch64", "no") =>
          Utils.getStream("conf/ml/linR/linR.yml")
        case ("x86_64", "no") =>
          Utils.getStream("conf/ml/linR/linR.yml")
        case ("x86_64", "yes") =>
          Utils.getStream("conf/ml/linR/linR_raw.yml")
      }
      val representer = new Representer
      representer.addClassTag(classOf[LinRParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[LinRConfig]), representer, options)
      val description = new TypeDescription(classOf[LinRParams])
      yaml.addTypeDescription(description)
      val configs: LinRConfig = yaml.load(stream).asInstanceOf[LinRConfig]
      val params = new LinRParams()

      val linRParamMap: util.HashMap[String, Object] = configs.linR.get(cpuName).get(datasetName)
      params.setPt(linRParamMap.getOrDefault("pt", "2000").asInstanceOf[Int])
      params.setNumFeatures(linRParamMap.getOrDefault("numFeatures", "500").asInstanceOf[Int])
      params.setLoss(linRParamMap.getOrDefault("loss", "squaredError").asInstanceOf[String])
      params.setRegParam(linRParamMap.getOrDefault("regParam", "0.01").asInstanceOf[Double])
      params.setElasticNetParam(linRParamMap.getOrDefault("elasticNetParam", "0.0").asInstanceOf[Double])
      params.setMaxIter(linRParamMap.getOrDefault("maxIter", "500").asInstanceOf[Int])
      params.setTolerance(linRParamMap.getOrDefault("tolerance", "1E-6").asInstanceOf[Double])

      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setApiName(apiName)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("LinR")

      var appName = s"LinR_${datasetName}_${apiName}"
      if (isRaw.equals("yes")){
        appName = s"LinR_RAW_${datasetName}_${apiName}"
      }
      params.setTestcaseType(appName)
      val conf = new SparkConf()
        .setAppName(appName).setMaster(master)
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

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/LinRegression_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s; evaluation: ${res}")
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

    val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
    for (i <- 0 to paramMaps.size -1) {
      paramMaps(i) = ParamMap(linR.maxIter -> params.maxIter)
        .put(linR.regParam, params.regParam)
    }

    val model = params.apiName match {
      case "fit" => linR.fit(trainingData)
      case "fit1" =>
        val models = linR.fit(trainingData, paramMaps)
        models(0)
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

    (res, costTime)
  }
}
