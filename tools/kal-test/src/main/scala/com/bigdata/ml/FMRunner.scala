package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.UpEvaluationVerify
import com.bigdata.compare.ml.DownEvaluationVerify

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, when}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.FMClassifier
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.regression.FMRegressor
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Paths, Files}
import java.util
import scala.beans.BeanProperty
import scala.io.Source

class FMConfig extends Serializable {
  @BeanProperty var fm: util.HashMap[String, util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]]] = _
}

class FMParams extends Serializable {
  @BeanProperty var pt: Int = _
  @BeanProperty var numFeatures: Int = _
  @BeanProperty var sparseOrDense: String = _
  @BeanProperty var regParam: Double = _
  @BeanProperty var numIterations: Int = _
  @BeanProperty var tolerance: Double = _

  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var algorithmType: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var evaluation: Double = _
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

object FMRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (algorithmType, datasetName, apiName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3), modelConfSplit(4))
      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))
      val cpuName = args(2)
      val sparkConfSplit = args(3).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))
      val saveResultPath = args(4)

      val stream = Utils.getStream("conf/ml/fm/fm.yml")
      val representer = new Representer
      representer.addClassTag(classOf[FMParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[FMConfig]), representer, options)
      val description = new TypeDescription(classOf[FMParams])
      yaml.addTypeDescription(description)
      val configs: FMConfig = yaml.load(stream).asInstanceOf[FMConfig]
      val params = new FMParams()
      val paramsMap: util.HashMap[String, Object] = configs.fm.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(algorithmType).get(datasetName)
      params.setPt(paramsMap.getOrDefault("pt", "276").asInstanceOf[Int])
      params.setNumFeatures(paramsMap.getOrDefault("numFeatures", "28").asInstanceOf[Int])
      params.setSparseOrDense(paramsMap.getOrDefault("sparseOrDense", "dense").asInstanceOf[String])
      params.setRegParam(paramsMap.getOrDefault("regParam", "0.0").asInstanceOf[Double])
      params.setNumIterations(paramsMap.getOrDefault("numIterations", "5000").asInstanceOf[Int])
      params.setTolerance(paramsMap.getOrDefault("tolerance", "1E-6").asInstanceOf[Double])
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setAlgorithmType(algorithmType)
      params.setApiName(apiName)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("FM")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${algorithmType}_${datasetName}_${apiName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${algorithmType}_${datasetName}_${apiName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${algorithmType}_${datasetName}_${apiName}_raw"
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
      if (isRaw.equals("no") && (datasetName.equals("higgs") || datasetName.equals("epsilon"))) {
        conf.set("spark.boostkit.mllib.optimization.LBFGSN.costFun.opt", "false")
      }
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val (res, costTime) = new FMKernel().fmDataframeJob(spark, params)
      params.setEvaluation(res)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        val isCorrect = params.algorithmType match {
          case "classification" => UpEvaluationVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark)
          case "regression" => DownEvaluationVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark)
        }
        params.setIsCorrect(isCorrect)
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

class FMKernel {
  def fmDataframeJob(spark: SparkSession, params: FMParams): (Double, Double) = {
    val sc = spark.sparkContext
    val pt = params.pt
    val algorithmType = params.algorithmType
    val trainingDataPath = params.trainingDataPath
    val testDataPath = params.testDataPath
    val numFeatures = params.numFeatures
    val sparseOrDense = params.sparseOrDense
    val regParam = params.regParam
    val numIterations = params.numIterations
    val tolerance = params.tolerance

    println(s"Initialized spark session.")
    val t1 = System.currentTimeMillis()

    import spark.implicits._
    val trainData = spark.read.format("libsvm").option("vectorType", sparseOrDense).option("numFeatures", numFeatures)
      .load(trainingDataPath).withColumn("label", when($"label" === -1.0, 0.0).otherwise($"label"))
      .repartition(pt).cache()
    println("trainData: " + trainData.count())
    val t2 = System.currentTimeMillis()
    println("* after preprocess:  " + t2)

    val fm = algorithmType match {
      case "classification" =>{
        val modelFM = new FMClassifier()
          .setRegParam(regParam)
          .setMaxIter(numIterations)
          .setTol(tolerance)
          .setSeed(-2050267832)
        modelFM
      }
      case "regression" =>{
        val modelFM = new FMRegressor()
          .setRegParam(regParam)
          .setMaxIter(numIterations)
          .setTol(tolerance)
          .setSeed(-2050267832)
        modelFM
      }
    }
    val model = fm.fit(trainData)
    val t3 = System.currentTimeMillis()
    println("* after train:       " + t3)

    val testData = spark.read.format("libsvm").option("vectorType", sparseOrDense).option("numFeatures", numFeatures)
      .load(testDataPath).withColumn("label", when($"label" === -1.0, 0.0).otherwise($"label"))
      .repartition(pt).cache()
    println("testData: " + testData.count())

    val getSquaredError = udf((v1: Double, v2: Double) => {
      math.pow((v1 - v2), 2)
    })
    // Evaluate model on training examples and compute training error
    val (res, t4) = algorithmType match {
      case "classification" =>{
        val valuesAndPreds = model.transform(testData)
        val accuracy = valuesAndPreds.filter($"label" === $"prediction").count().toDouble / valuesAndPreds.count
        val t4 = System.currentTimeMillis()
        println("* after predict:     " + t4)

        println("\n--------success--------\n")

        val valuesAndPreds1 = model.transform(trainData)
        val accuracy1 = valuesAndPreds1.filter($"label" === $"prediction").count().toDouble / valuesAndPreds1.count
        println("trainData accuracy = " + accuracy1)
        println("testData  accuracy = " + accuracy)
        (accuracy, t4)
      }
      case "regression" =>{
        val valuesAndPreds = model.transform(testData)
          .withColumn("squaredError", getSquaredError($"label", $"prediction"))
          .select("squaredError").summary("mean")
        val rmse = math.sqrt(valuesAndPreds.select("squaredError").first().getString(0).toDouble)
        val t4 = System.currentTimeMillis()
        println("* after predict:     " + t4)
        println("\n--------success--------\n")
        val valuesAndPreds1 = model.transform(trainData)
          .withColumn("squaredError", getSquaredError($"label", $"prediction"))
          .select("squaredError").summary("mean")
        val rmse1 = math.sqrt(valuesAndPreds1.select("squaredError").first().getString(0).toDouble)
        println("trainData rmse = " + rmse1)
        println("testData  rmse = " + rmse)
        (rmse, t4)
      }
    }

    val trainingProcess = (t3 - t1).toDouble / 1000
    val trainingStep = (t3 - t2).toDouble / 1000
    val dataProcess = (t2 - t1).toDouble / 1000
    val predict = (t4 - t3).toDouble / 1000
    println("[s]train total:     " + trainingProcess)
    println("[s]data preprocess: " + dataProcess)
    println("[s]train:           " + trainingStep)
    println("[s]predict:         " + predict)

    Utils.saveEvaluation(res, params.saveDataPath, sc)
    (res, trainingProcess)
  }
}
