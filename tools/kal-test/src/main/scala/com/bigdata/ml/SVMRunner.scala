package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.UpEvaluationVerify

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.linalg.Vectors
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.util
import java.io.{File, FileWriter}
import java.util.HashMap
import scala.beans.BeanProperty

class SVMConfig extends Serializable {
  @BeanProperty var svm: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class SVMParams extends Serializable {
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var regParam: Double = _
  @BeanProperty var maxIter: Int = _

  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var evaluation: Double = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object SVMRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, apiName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))
      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/svm/svm.yml")
      val representer = new Representer
      representer.addClassTag(classOf[SVMParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SVMConfig]), representer, options)
      val description = new TypeDescription(classOf[SVMParams])
      yaml.addTypeDescription(description)
      val config: SVMConfig = yaml.load(stream).asInstanceOf[SVMConfig]
      val paramsMap: util.HashMap[String, Object] = config.svm.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new SVMParams()
      params.setNumPartitions(paramsMap.get("numPartitions").asInstanceOf[Int])
      params.setMaxIter(paramsMap.get("maxIter").asInstanceOf[Int])
      params.setRegParam(paramsMap.get("regParam").asInstanceOf[Double])
      params.setApiName(apiName)
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setCpuName(cpuName)
      params.setDatasetName(datasetName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("SVM")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}_${apiName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${datasetName}_${apiName}"
      if (isRaw.equals("yes")) {
        appName = s"${params.algorithmName}_${datasetName}_${apiName}_raw"
        params.setVerifiedDataPath(params.saveDataPath)
        params.setSaveDataPath(s"${params.saveDataPath}_raw")
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName)
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val (evaluation, costTime) = new SVMKernel().runJob(spark, params)
      params.setEvaluation(evaluation)
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
      println(s"Exec Successful: costTime: ${costTime}s; evaluation: ${evaluation};isCorrect: ${params.isCorrect}")
    }catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

class SVMKernel {
  def runJob(spark: SparkSession,params: SVMParams): (Double, Double) = {
    import spark.implicits._
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()
    val training = sc.textFile(params.trainingDataPath).repartition(params.numPartitions)
    val test = sc.textFile(params.testDataPath).repartition(params.numPartitions)
    val parsedData0 = training.map { line =>
      val parts = line.split(',')
      (parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }
      .persist()
    val parsedData = parsedData0.toDF("label", "features")
    val svm = new LinearSVC()
      .setMaxIter(params.maxIter)
      .setRegParam(params.regParam)

    val paramMap = ParamMap(svm.maxIter -> params.maxIter)
      .put(svm.regParam, params.regParam)
    val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
    for (i <- 0 to paramMaps.size -1) {
      paramMaps(i) = ParamMap(svm.maxIter -> params.maxIter)
        .put(svm.regParam, params.regParam)
    }
    val maxIterParamPair = ParamPair(svm.maxIter, params.maxIter)
    val regParamPair = ParamPair(svm.regParam, params.regParam)
    val model = params.apiName match {
      case "fit" => svm.fit(parsedData)
      case "fit1" => svm.fit(parsedData, paramMap)
      case "fit2" =>
        val models = svm.fit(parsedData, paramMaps)
        models(0)
      case "fit3" => svm.fit(parsedData, maxIterParamPair, regParamPair)
    }
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val parsedTest = test.map { line =>
      val parts = line.split(',')
      (parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }
      .toDF("label","features")
      .cache()

    val result = model.transform(parsedTest)
    val res = result.filter($"label"===$"prediction").count().toDouble/result.count
    Utils.saveEvaluation(res, params.saveDataPath, sc)
    (res, costTime)
  }
}