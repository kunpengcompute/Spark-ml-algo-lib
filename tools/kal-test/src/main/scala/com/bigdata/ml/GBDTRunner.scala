package com.bigdata.ml

import java.io.FileWriter
import java.io.File
import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.param.{ParamPair, ParamMap}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees

import com.bigdata.utils.Utils

import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class GBDTConfig extends Serializable {

  @BeanProperty var gbdt: HashMap[String, Object] = _
}

class GBDTParams extends Serializable {

  @BeanProperty var numPartitions: Int = _
  @BeanProperty var maxIter: Int = _
  @BeanProperty var maxDepth: Int = _
  @BeanProperty var maxBins: Int = _
  @BeanProperty var stepSize: Double = _
  @BeanProperty var cacheNodeIds: Boolean = _
  @BeanProperty var maxMemoryInMB: Int = _
  @BeanProperty var minInstancesPerNode: Int = _
  @BeanProperty var minInfoGain: Double = _
  @BeanProperty var subsamplingRate: Double = _
  @BeanProperty var featureSubsetStrategy: String = _
  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var algorithmType: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var datasetCpuName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var evaluation: Double = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}


class GBDTKernel {
  
  def gbdtDataframeJob(spark: SparkSession, params: GBDTParams): (Double, Double) = {

    val startTime = System.currentTimeMillis()

    val trainingData = spark
      .read
      .format("libsvm")
      .option("vectorType", "dense")
      .load(params.trainingDataPath)
      .repartition(params.numPartitions)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(trainingData)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setHandleInvalid("skip")
      .setMaxCategories(2)
      .fit(trainingData)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val gbdt = params.algorithmType match {
      case "classification" =>{
        new GBTClassifier()
          .setLabelCol("indexedLabel")
          .setFeaturesCol("indexedFeatures")
          .setMaxIter(params.maxIter)
          .setMaxDepth(params.maxDepth)
          .setMaxBins(params.maxBins)
          .setStepSize(params.stepSize)
          .setMinInstancesPerNode(params.minInstancesPerNode)
          .setMinInfoGain(params.minInfoGain)
          .setMaxMemoryInMB(params.maxMemoryInMB)
          .setSubsamplingRate(params.subsamplingRate)
          .setCacheNodeIds(params.cacheNodeIds)
          .setFeatureSubsetStrategy(params.featureSubsetStrategy)
          .setSeed(2020)
      }
      case "regression" =>{
        new GBTRegressor()
          .setLabelCol("indexedLabel")
          .setFeaturesCol("indexedFeatures")
          .setMaxIter(params.maxIter)
          .setMaxDepth(params.maxDepth)
          .setMaxBins(params.maxBins)
          .setStepSize(params.stepSize)
          .setMinInstancesPerNode(params.minInstancesPerNode)
          .setMinInfoGain(params.minInfoGain)
          .setMaxMemoryInMB(params.maxMemoryInMB)
          .setSubsamplingRate(params.subsamplingRate)
          .setCacheNodeIds(params.cacheNodeIds)
          .setFeatureSubsetStrategy(params.featureSubsetStrategy)
          .setSeed(2020)
      }
    }

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbdt, labelConverter))

    val paramMap = ParamMap(gbdt.maxDepth -> params.maxDepth)
      .put(gbdt.maxIter, params.maxIter)

    val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
    for (i <- 0 to paramMaps.size -1) {
      paramMaps(i) = ParamMap(gbdt.maxDepth -> params.maxDepth)
        .put(gbdt.maxIter, params.maxIter)
    }

    val maxDepthParamPair = ParamPair(gbdt.maxDepth, params.maxDepth)
    val maxIterParamPair = ParamPair(gbdt.maxIter, params.maxIter)
    val maxBinsParamPair = ParamPair(gbdt.maxBins, params.maxBins)

    val model = params.apiName match {
      case "fit" => pipeline.fit(trainingData)
      case "fit1" => pipeline.fit(trainingData, paramMap)
      case "fit2" =>
        val models = pipeline.fit(trainingData, paramMaps)
        models(0)
      case "fit3" => pipeline.fit(trainingData, maxDepthParamPair, maxIterParamPair, maxBinsParamPair)

    }

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val testData = spark
      .read
      .format("libsvm")
      .option("vectorType", "dense")
      .load(params.testDataPath)
      .repartition(params.numPartitions)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select (prediction, true label) and compute test error.
    val evaluator = params.algorithmType match {
      case "classification" =>
        new MulticlassClassificationEvaluator()
          .setLabelCol ("indexedLabel")
          .setPredictionCol ("prediction")
          .setMetricName ("accuracy")

      case "regression" =>
        new RegressionEvaluator()
          .setLabelCol ("indexedLabel")
          .setPredictionCol ("prediction")
          .setMetricName ("rmse")
    }
    val res = evaluator.evaluate(predictions)
    (res, costTime)
}

  def gbdtRDDJob(spark: SparkSession, params: GBDTParams): (Double, Double) = {

    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()

    val trainingData = MLUtils.loadLibSVMFile(sc, params.trainingDataPath).repartition(params.numPartitions)
    val trainingLabelPositive = trainingData.map(i => if (i.label < 0) {
      LabeledPoint(0.0, i.features)
    } else {
      LabeledPoint(i.label, i.features)
    })

    val boostingStrategy = params.algorithmType match {
      case "classification" => BoostingStrategy.defaultParams("Classification")
      case "regression" => BoostingStrategy.defaultParams("Regression")
    }

    boostingStrategy.numIterations = params.maxIter
    boostingStrategy.learningRate = params.stepSize
    boostingStrategy.treeStrategy.maxDepth = params.maxDepth
    boostingStrategy.treeStrategy.maxBins = params.maxBins
    boostingStrategy.treeStrategy.minInstancesPerNode = params.minInstancesPerNode
    boostingStrategy.treeStrategy.maxMemoryInMB = params.maxMemoryInMB
    boostingStrategy.treeStrategy.subsamplingRate = params.subsamplingRate
    boostingStrategy.treeStrategy.useNodeIdCache = params.cacheNodeIds

    val gbdt = new GradientBoostedTrees(boostingStrategy)
    val model = params.apiName match {
      case "rdd" => gbdt.run(trainingLabelPositive)
      case "javardd" => gbdt.run(trainingLabelPositive.toJavaRDD)
    }

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val testData = MLUtils.loadLibSVMFile(sc, params.testDataPath).repartition(params.numPartitions)
    val testLabelPositive = testData.map(i => if (i.label < 0) {
      LabeledPoint(0.0, i.features)
    } else {
      LabeledPoint(i.label, i.features)
    })

    val labeleAndPreds = testLabelPositive.map{ point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val res = params.algorithmType match {
      case "classification" => labeleAndPreds.filter(r => r._1 == r._2).count.toDouble / testLabelPositive.count()
      case "regression" => math.sqrt(labeleAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.mean())
    }
    (res, costTime)
  }
}



object GBDTRunner {
  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (algorithmType, dataStructure, datasetName, apiName, cpuName) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3), modelConfSplit(4))

      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))

      val datasetCpuName = s"${datasetName}_${cpuName}"
      val stream = Utils.getStream("conf/ml/gbdt/gbdt.yml")
      val representer = new Representer
      representer.addClassTag(classOf[GBDTParams], Tag.MAP)

      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[GBDTConfig]), representer, options)
      val description = new TypeDescription(classOf[GBDTParams])
      yaml.addTypeDescription(description)
      val config: GBDTConfig = yaml.load(stream).asInstanceOf[GBDTConfig]
      val paramsMap = config.gbdt.get(datasetCpuName).asInstanceOf[HashMap[String, Object]]

      val params = new GBDTParams()

      params.setAlgorithmType(algorithmType)
      params.setApiName(apiName)
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setDatasetName(datasetName)
      params.setDatasetCpuName(datasetCpuName)
      params.setAlgorithmName("GBDT")
      if (cpuName == "raw") {
        params.setIsRaw("yes")
      }

      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)
      params.setMaxIter(paramsMap.get("maxIter").toString.toInt)
      params.setMaxDepth(paramsMap.get("maxDepth").toString.toInt)
      params.setMaxBins(paramsMap.get("maxBins").toString.toInt)
      params.setStepSize(paramsMap.get("stepSize").toString.toDouble)
      params.setCacheNodeIds(paramsMap.get("cacheNodeIds").toString.toBoolean)
      params.setMaxMemoryInMB(paramsMap.get("maxMemoryInMB").toString.toInt)
      params.setMinInstancesPerNode(paramsMap.get("minInstancesPerNode").toString.toInt)
      params.setMinInfoGain(paramsMap.get("minInfoGain").toString.toDouble)
      params.setSubsamplingRate(paramsMap.get("subsamplingRate").toString.toDouble)
      params.setFeatureSubsetStrategy(paramsMap.get("featureSubsetStrategy").toString)

      var appName = s"GBDT_${algorithmType}_${dataStructure}_${datasetName}_${apiName}"
      if (cpuName.equals("raw")){
        appName = s"GBDT_RAW_${algorithmType}_${dataStructure}_${datasetName}_${apiName}"
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf()
        .setAppName(appName)
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val (res, costTime) = dataStructure match {
        case "dataframe" => new GBDTKernel().gbdtDataframeJob(spark, params)
        case "rdd" => new GBDTKernel().gbdtRDDJob(spark, params)
      }
      params.setEvaluation(res)
      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/GBDT_${
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