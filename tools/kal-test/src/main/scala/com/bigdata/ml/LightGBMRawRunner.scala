package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.UpEvaluationVerify
import com.bigdata.compare.ml.DownEvaluationVerify

import com.microsoft.ml.spark.core.metrics.MetricConstants
import com.microsoft.ml.spark.train.ComputeModelStatistics
import com.microsoft.ml.spark.lightgbm.{LightGBMClassifier, LightGBMRegressor}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.lang.System.nanoTime
import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Paths, Files}
import java.util
import scala.beans.BeanProperty
import scala.util.Random

class LightGBMRawConfig extends Serializable {
  @BeanProperty var lgbm: util.HashMap[String, util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]]] = _
}

class LightGBMRawParams extends Serializable {
  @BeanProperty var objective: String = _
  @BeanProperty var labelCol: String = _
  @BeanProperty var featuresCol: String = _
  @BeanProperty var verbosity: Int  = _
  @BeanProperty var learningRate: Double = _
  @BeanProperty var maxDepth: Int = _
  @BeanProperty var maxBin: Int = _
  @BeanProperty var numIterations: Int = _
  @BeanProperty var numTasks: Int = _
  @BeanProperty var minGainToSplit: Double = _
  @BeanProperty var lambdaL2: Double = _
  @BeanProperty var numLeaves: Int = _
  @BeanProperty var minSumHessianInLeaf: Double = _
  @BeanProperty var minDataInLeaf: Int = _
  @BeanProperty var baggingFraction: Double = _
  @BeanProperty var baggingFreq: Int = _
  @BeanProperty var numThreads: Int = _
  @BeanProperty var networkCompression: Int = _
  @BeanProperty var histSynchAlgo: Int = _
  @BeanProperty var loglossApx: Int = _
  @BeanProperty var loglossApxEps: Double = _
  @BeanProperty var loadingBalance: String = _

  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var algorithmType: String = _
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

object LightGBMRawRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (algorithmType, datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))
      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/lgbm/lgbm.yml")
      val representer = new Representer
      representer.addClassTag(classOf[LightGBMRawParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[LightGBMRawConfig]), representer, options)
      val description = new TypeDescription(classOf[LightGBMRawParams])
      yaml.addTypeDescription(description)
      val configs: LightGBMRawConfig = yaml.load(stream).asInstanceOf[LightGBMRawConfig]
      val params = new LightGBMRawParams()
      val paramsMap: util.HashMap[String, Object] = configs.lgbm.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(algorithmType).get(datasetName)
      params.setObjective(paramsMap.get("objective").asInstanceOf[String])
      params.setLabelCol(paramsMap.get("labelCol").asInstanceOf[String])
      params.setFeaturesCol(paramsMap.get("featuresCol").asInstanceOf[String])
      params.setVerbosity(paramsMap.get("verbosity").asInstanceOf[Int])
      params.setLearningRate(paramsMap.get("eta").asInstanceOf[Double])
      params.setMaxDepth(paramsMap.get("max_depth").asInstanceOf[Int])
      params.setMaxBin(paramsMap.get("max_bin").asInstanceOf[Int])
      params.setNumIterations(paramsMap.get("num_round").asInstanceOf[Int])
      params.setNumTasks(paramsMap.get("num_tasks").asInstanceOf[Int])
      params.setMinGainToSplit(paramsMap.get("min_gain_to_split").asInstanceOf[Double])
      params.setLambdaL2(paramsMap.get("lambda_l2").asInstanceOf[Double])
      params.setNumLeaves(paramsMap.get("num_leaves").asInstanceOf[Int])
      params.setMinSumHessianInLeaf(paramsMap.get("min_child_weight").asInstanceOf[Double])
      params.setMinDataInLeaf(paramsMap.get("min_data_in_leaf").asInstanceOf[Int])
      params.setBaggingFraction(paramsMap.get("bagging").asInstanceOf[Double])
      params.setBaggingFreq(paramsMap.get("bagging_freq").asInstanceOf[Int])
      params.setNumThreads(paramsMap.get("num_threads").asInstanceOf[Int])
      params.setNetworkCompression(paramsMap.get("network_compression").asInstanceOf[Int])
      params.setHistSynchAlgo(paramsMap.get("hist_synch_algo").asInstanceOf[Int])
      params.setLoglossApx(paramsMap.get("logloss_apx").asInstanceOf[Int])
      params.setLoglossApxEps(paramsMap.get("logloss_apx_eps").asInstanceOf[Double])
      params.setLoadingBalance(paramsMap.get("loading_balance").asInstanceOf[String])
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setAlgorithmType(algorithmType)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("LightGBM")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${algorithmType}_${datasetName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${algorithmType}_${datasetName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${algorithmType}_${datasetName}_raw"
        params.setVerifiedDataPath(params.saveDataPath)
        params.setSaveDataPath(s"${params.saveDataPath}_raw")
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName)
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val (res, costTime) = new LightGBMRawKernel().runJob(spark, params)
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

class LightGBMRawKernel{
  def runJob(spark: SparkSession, params: LightGBMRawParams): (Double, Double) = {
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    println(s"Initialized spark session.")
    val t1 = System.currentTimeMillis()

    import spark.implicits._
    val trainData = spark.read.format("libsvm").option("vectorType", "sparse")
      .load(params.trainingDataPath)
      .repartition(params.numTasks)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val t2 = System.currentTimeMillis()
    println("* after preprocess:  " + t2)

    val lgbm = params.algorithmType match {
      case "classification" =>{
        val classifier = new LightGBMClassifier()
          .setObjective(params.objective)
          .setLabelCol(params.labelCol)
          .setFeaturesCol(params.featuresCol)
          .setVerbosity(params.verbosity)
          .setNumIterations(params.numIterations)
          .setMaxDepth(params.maxDepth)
          .setLearningRate(params.learningRate)
          .setNumTasks(params.numTasks)
          .setMaxBin(params.maxBin)
          .setMinGainToSplit(params.minGainToSplit)
          .setLambdaL2(params.lambdaL2)
          .setNumLeaves(params.numLeaves)
          .setMinDataInLeaf(params.minDataInLeaf)
          .setMinSumHessianInLeaf(params.minSumHessianInLeaf)
          .setBaggingFraction(params.baggingFraction)
          .setBaggingFreq(params.baggingFreq)
        classifier
      }
      case "regression" =>{
        val regressor = new LightGBMRegressor()
          .setObjective(params.objective)
          .setLabelCol(params.labelCol)
          .setFeaturesCol(params.featuresCol)
          .setVerbosity(params.verbosity)
          .setNumIterations(params.numIterations)
          .setMaxDepth(params.maxDepth)
          .setLearningRate(params.learningRate)
          .setNumTasks(params.numTasks)
          .setMaxBin(params.maxBin)
          .setMinGainToSplit(params.minGainToSplit)
          .setLambdaL2(params.lambdaL2)
          .setNumLeaves(params.numLeaves)
          .setMinDataInLeaf(params.minDataInLeaf)
          .setMinSumHessianInLeaf(params.minSumHessianInLeaf)
          .setBaggingFraction(params.baggingFraction)
          .setBaggingFreq(params.baggingFreq)
        regressor
      }
    }
    val model = lgbm.fit(trainData)
    val t3 = System.currentTimeMillis()
    println("* after train:       " + t3)

    val testData = spark.read.format("libsvm").option("vectorType", "sparse")
      .load(params.testDataPath)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"Test data read successful. Number of partitions - ${testData.rdd.getNumPartitions}")
    val predictions = model.transform(testData)
    val (res, t4) = params.algorithmType match {
      case "classification" =>{
        val metrics = new ComputeModelStatistics()
          .setLabelCol("label")
          .setScoresCol("probability")
          .setScoredLabelsCol("prediction")
          .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
          .transform(predictions)
        val ecc = metrics.collect().apply(0).apply(1).asInstanceOf[Double]
        val t4 = System.currentTimeMillis()
        (ecc, t4)
      }
      case "regression" =>{
        // compute model metrics
        val metrics = new ComputeModelStatistics()
          .setEvaluationMetric("regression")
          .setLabelCol("label")
          .setScoresCol("prediction")
          .transform(predictions)
        // print metrics
        val mse = metrics.collect().apply(0).apply(0).asInstanceOf[Double]
        val t4 = System.currentTimeMillis()
        (mse, t4)
      }
    }
    println("Model predictions:")
    predictions.select("prediction", "label", "features").show(5)
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