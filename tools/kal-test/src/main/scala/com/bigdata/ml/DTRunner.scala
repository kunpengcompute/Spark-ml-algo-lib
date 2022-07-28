package com.bigdata.ml
import java.io.{File, FileWriter}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.storage.StorageLevel

class DTConfig extends Serializable {
  @BeanProperty var dt: util.HashMap[String, util.HashMap[String, util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]]]] = _
}

class DTParams extends Serializable {
  @BeanProperty var genericPt: Int = _
  @BeanProperty var maxMemoryInMB: Int = _
  @BeanProperty var pt: Int = _
  @BeanProperty var numCopiesInput: Int = _
  @BeanProperty var maxDepth: Int = _
  @BeanProperty var maxBins: Int = _
  @BeanProperty var numClasses: Int = _
  @BeanProperty var useNodeIdCache: Boolean = _
  @BeanProperty var checkpointInterval: Int = _
  @BeanProperty var featuresType: String = _
  @BeanProperty var bcVariables: Boolean = _
  @BeanProperty var copyStrategy: String = _
  @BeanProperty var useDFCollPtner: String = _

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
}

object DTRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("_")
      val (algorithmType, dataStructure, datasetName, apiName) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))

      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))

      val cpuName = args(2)
      val isRaw = args(3)
      val sparkConfSplit = args(4).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))

      val stream = Utils.getStream("conf/ml/dt/dt.yml")

      val representer = new Representer
      representer.addClassTag(classOf[DTParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[DTConfig]), representer, options)
      val description = new TypeDescription(classOf[DTParams])
      yaml.addTypeDescription(description)

      val configs: DTConfig = yaml.load(stream).asInstanceOf[DTConfig]
      val params = new DTParams()

      val dtParamMap: util.HashMap[String, Object] = configs.dt.get(isRaw match {
        case "no" => "opt"
        case _ => "raw"
      }).get(algorithmType).get(dataStructure).get(datasetName)
      params.setGenericPt(dtParamMap.getOrDefault("genericPt", "1000").asInstanceOf[Int])
      params.setMaxMemoryInMB(dtParamMap.getOrDefault("maxMemoryInMB", "256").asInstanceOf[Int])
      params.setPt(dtParamMap.getOrDefault("pt", "1000").asInstanceOf[Int])
      params.setNumCopiesInput(dtParamMap.getOrDefault("numCopiesInput", "1").asInstanceOf[Int])
      params.setMaxDepth(dtParamMap.getOrDefault("maxDepth", "5").asInstanceOf[Int])
      params.setMaxBins(dtParamMap.getOrDefault("maxBins", "32").asInstanceOf[Int])
      params.setNumClasses(dtParamMap.get("numClasses").asInstanceOf[Int])
      params.setUseNodeIdCache(dtParamMap.getOrDefault("useNodeIdCache", "false").asInstanceOf[Boolean])
      params.setCheckpointInterval(dtParamMap.getOrDefault("checkpointInterval", "10").asInstanceOf[Int])
      params.setFeaturesType(dtParamMap.getOrDefault("featuresType", "array").asInstanceOf[String])
      params.setBcVariables(dtParamMap.getOrDefault("bcVariables", "false").asInstanceOf[Boolean])
      params.setCopyStrategy(dtParamMap.getOrDefault("copyStrategy", "normal").asInstanceOf[String])
      params.setUseDFCollPtner(dtParamMap.getOrDefault("useDFCollPtner", "true").asInstanceOf[String])
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setAlgorithmType(algorithmType)
      params.setApiName(apiName)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("DT")

      var appName = s"DT_${algorithmType}_${datasetName}_${dataStructure}_${apiName}"
      if (isRaw.equals("yes")){
        appName = s"DT_RAW_${algorithmType}_${datasetName}_${dataStructure}_${apiName}"
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
      if ("no" == isRaw.asInstanceOf[String]) {
        conf.set("spark.boostkit.ml.rf.binnedFeaturesDataType",
          dtParamMap.get("featuresType").asInstanceOf[String])
        conf.set("spark.boostkit.ml.rf.numTrainingDataCopies",
          dtParamMap.get("numCopiesInput").asInstanceOf[Int].toString)
        conf.set("spark.boostkit.ml.rf.numPartsPerTrainingDataCopy",
          dtParamMap.get("pt").asInstanceOf[Int].toString)
        conf.set("spark.boostkit.ml.rf.broadcastVariables",
          dtParamMap.get("bcVariables").asInstanceOf[Boolean].toString)
        conf.set("spark.boostkit.ml.rf.copyStrategy",
          dtParamMap.get("copyStrategy").asInstanceOf[String])
        conf.set("spark.boostkit.ml.rf.useDFCollPartitioner",
          dtParamMap.get("useDFCollPtner").asInstanceOf[String])
        if (dataStructure == "rdd") {
          conf.set("spark.boostkit.ml.rf.maxBins",
            dtParamMap.get("maxBins").asInstanceOf[Int].toString)
          conf.set("spark.boostkit.ml.rf.maxMemoryInMB",
            dtParamMap.get("maxMemoryInMB").asInstanceOf[Int].toString)
        }
      }
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val (res, costTime) = dataStructure match {
        case "dataframe" => new DTKernel().dtDataframeJob(spark, params)
        case "rdd" => new DTKernel().dtRDDJob(spark, params)
      }
      params.setEvaluation(res)
      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/DT_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")

      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s; evaluation: ${res}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
    }
  }
}

class DTKernel {
  def dtDataframeJob(spark: SparkSession, params: DTParams): (Double, Double) = {
    val pt = params.pt
    val trainingDataPath = params.trainingDataPath
    val testDataPath = params.testDataPath
    val maxDepth = params.maxDepth
    val maxBins = params.maxBins
    val useNodeIdCache = params.useNodeIdCache
    val checkpointInterval = params.checkpointInterval
    val maxMemoryInMB = params.maxMemoryInMB
    val genericPt = params.genericPt

    println(s"Initialized spark session.")
    val startTime = System.currentTimeMillis()

    val indexLabel: Boolean = params.datasetName match {
      case "mnist8m" =>
        false
      case "higgs" =>
        false
      case "epsilon" =>
        true
      case "rcv" =>
        true
      case _ =>
        true
    }
    var indexLabelDone = false
    val trainingLabelColName = if (indexLabel) "indexedLabel" else "label"

    val reader = spark.read.format("libsvm")
    if (params.datasetName == "mnist8m") {
      reader.option("numFeatures",784)
    } else if (params.datasetName == "higgs") {
      reader.option("numFeatures",28)
    } else if (params.datasetName == "epsilon") {
      reader.option("numFeatures", 2000)
    } else if (params.datasetName == "rcv") {
      reader.option("numFeatures", 47236)
    }

    val numPtTrainData = if ("no" == params.isRaw) genericPt else pt
    println(s"numPtTrainData = ${(numPtTrainData)}")
    val trainingData = {
      var trainingData = reader
      .load(trainingDataPath)
      .repartition(numPtTrainData)
      if (indexLabel && params.algorithmType == "classification") {
        params.datasetName match {
          case "epsilon" | "rcv" =>
            trainingData = trainingData.selectExpr("if(label < 0.0, 0, 1) as indexedLabel", "features")
            indexLabelDone = true
          case _ =>
            println(s"index Label by StringIndexer because of unknown dataset")
        }
      }
      trainingData.persist(StorageLevel.MEMORY_AND_DISK_SER)
      trainingData
    }

    //for implementing different fit APIs
    val maxBinsJY = 10
    val maxDepthJY = 3

    // Train a RandomForest model
    val dTree = params.algorithmType match {
      case "classification" =>{
        val oldDt = new DecisionTreeClassifier()
          .setLabelCol(trainingLabelColName)
          .setFeaturesCol("features")
          .setMaxDepth(maxDepthJY)
          .setMaxBins(maxBinsJY)
          .setCacheNodeIds(useNodeIdCache)
          .setCheckpointInterval(checkpointInterval)
          .setMaxMemoryInMB(maxMemoryInMB)
        if (params.apiName == "fit"){
          oldDt.setMaxBins(maxBins)
          oldDt.setMaxDepth(maxDepth)
        }
        oldDt
      }
      case "regression" =>{
        val oldDt = new DecisionTreeRegressor()
          .setLabelCol("label")
          .setFeaturesCol("features")
          .setMaxDepth(maxDepthJY)
          .setMaxBins(maxBinsJY)
          .setCacheNodeIds(useNodeIdCache)
          .setCheckpointInterval(checkpointInterval)
          .setMaxMemoryInMB(maxMemoryInMB)
        if (params.apiName == "fit"){
          oldDt.setMaxBins(maxBins)
          oldDt.setMaxDepth(maxDepth)
        }
        oldDt
      }
    }

    val pipeline = if (!indexLabelDone && params.algorithmType == "classification") {
      val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(trainingData)
      new Pipeline()
      .setStages(Array(labelIndexer, dTree))
    } else {
      new Pipeline()
        .setStages(Array(dTree))
    }

    val paramMap = ParamMap(dTree.maxDepth ->params. maxDepth)
      .put(dTree.maxBins, params.maxBins)

    val paramMaps = new Array[ParamMap](2)
    for (i <- 0 until paramMaps.size){
      paramMaps(i) = ParamMap(dTree.maxDepth -> params.maxDepth)
        .put(dTree.maxBins, params.maxBins)
    }

    val maxDepthParamPair = ParamPair(dTree.maxDepth, params.maxDepth)
    val maxBinsParamPair = ParamPair(dTree.maxBins, params.maxBins)

    val model = params.apiName match {
      case "fit1" => pipeline.fit(trainingData, paramMap)
      case "fit2" =>
        val models = pipeline.fit(trainingData, paramMaps)
        models(0)
      case "fit3" => pipeline.fit(trainingData, maxDepthParamPair, maxBinsParamPair)
      case _ => pipeline.fit(trainingData)
    }

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val testData = {
      var testData = reader
        .load(testDataPath)
        .repartition(genericPt)
      if (indexLabel && params.algorithmType == "classification") {
        params.datasetName match {
          case "epsilon" | "rcv" =>
            testData = testData.selectExpr("if(label < 0.0, 0, 1) as indexedLabel", "features")
          case _ =>
            println(s"index Label by StringIndexer because of unknown dataset")
        }
      }
      testData.persist(StorageLevel.MEMORY_AND_DISK_SER)
      testData
    }

    // Make predictions.
    val predictions = model.transform(testData)

    // Select (prediction, true label) and compute test error.
    val evaluator = params.algorithmType match {
      case "classification" =>
        new MulticlassClassificationEvaluator()
          .setLabelCol (trainingLabelColName)
          .setPredictionCol ("prediction")
          .setMetricName ("accuracy")

      case "regression" =>
        new RegressionEvaluator()
          .setLabelCol ("label")
          .setPredictionCol ("prediction")
          .setMetricName ("rmse")
    }
    val res = evaluator.evaluate(predictions)
    (res, costTime)
  }

  def dtRDDJob(spark: SparkSession, params: DTParams): (Double, Double) = {

    val pt = params.pt
    val trainingDataPath = params.trainingDataPath
    val testDataPath = params.testDataPath
    var maxDepth = params.maxDepth
    val maxBins = params.maxBins
    val genericPt = params.genericPt
    var numClasses = params.numClasses

    val sc = spark.sparkContext
    println(s"Initialized spark session.")
    val startTime = System.currentTimeMillis()

    val numFeatures = params.datasetName match {
      case "mnist8m" => 784
      case "higgs" => 28
      case "epsilon" =>2000
      case "rcv" => 47236
    }

    val numPtTrainData = if ("no" == params.isRaw) genericPt else pt
    val trainingData = MLUtils.loadLibSVMFile(sc, trainingDataPath, numFeatures)
      .repartition(numPtTrainData)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val trainingLabelPositive = trainingData.map(i=> if (i.label < 0) {
      LabeledPoint(0.0, i.features)
    } else {
      LabeledPoint (i.label, i.features)
    })


    val model = params.algorithmType match {
      case "classification" =>
        DecisionTree.trainClassifier(trainingLabelPositive, numClasses, Map.empty[Int, Int], "gini", maxDepth, maxBins)
      case "regression" =>
        DecisionTree.trainRegressor(trainingLabelPositive, Map.empty[Int, Int], "variance", maxDepth, maxBins)
    }

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val testData = MLUtils.loadLibSVMFile(sc, testDataPath, numFeatures)
      .repartition(genericPt)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val testLabelPositive = testData.map(i=> if (i.label < 0) {
      LabeledPoint(0.0, i.features)
    } else {
      LabeledPoint (i.label, i.features)
    })

    val labeleAndPreds = testLabelPositive.map{ point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val res = params.algorithmType match {
      case "classification" => 1.0 - labeleAndPreds.filter(r => r._1 == r._2).count.toDouble / testLabelPositive.count()
      case "regression" => math.sqrt(labeleAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.mean())
    }
    (res, costTime)
  }

}
