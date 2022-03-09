package com.bigdata.ml

import java.io.{File, FileWriter}
import java.util.HashMap

import com.bigdata.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{LDA => MLLDA, LDAModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector => MLlibVector}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.clustering.{LocalLDAModel, LDA => MLlibLDA}
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty


class LDAConfig extends Serializable {

  @BeanProperty var lda: HashMap[String, Object] = _
}

class LDAParams extends Serializable {

  @BeanProperty var numPartitions: Int = _
  @BeanProperty var numPartitionsTest: Int = _
  @BeanProperty var numFeatures: Int = _
  @BeanProperty var maxIter: Int = _
  @BeanProperty var checkpointInterval: Int = _
  @BeanProperty var k: Int = _
  @BeanProperty var inputDataType: String = _
  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var dataStructure: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var datasetCpuName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var evaluation: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
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

    val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
    for (i <- 0 to paramMaps.size - 1) {
      paramMaps(i) = ParamMap(lda.k -> params.k)
        .put(lda.maxIter, params.maxIter)
    }
    val model = params.apiName match {
      case "fit" => lda.fit(trainingData)
      case "fit1" =>
        val models = lda.fit(trainingData, paramMaps)
        models(0)
    }
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    println("load and train costTime: " + costTime)

    trainingData.unpersist()
    spark.close()
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
    spark.close()
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
    spark.close()
    costTime
  }
}


object LDARunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (dataStructure, datasetName, apiName, cpuName) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))

      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))

      val datasetCpuName = s"${datasetName}_${cpuName}"
      val stream = Utils.getStream("conf/ml/lda/lda.yml")

      val representer = new Representer
      representer.addClassTag(classOf[LDAParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[LDAConfig]), representer, options)
      val description = new TypeDescription(classOf[LDAParams])
      yaml.addTypeDescription(description)
      val config: LDAConfig = yaml.load(stream).asInstanceOf[LDAConfig]
      val paramsMap = config.lda.get(datasetCpuName).asInstanceOf[HashMap[String, Object]]

      val params = new LDAParams()

      params.setApiName(apiName)
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setDatasetName(datasetName)
      params.setDatasetCpuName(datasetCpuName)
      params.setDataStructure(dataStructure)
      params.setAlgorithmName("LDA")
      if (cpuName == "raw") {
        params.setIsRaw("yes")
      }

      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)
      params.setNumPartitionsTest(paramsMap.get("numPartitionsTest").toString.toInt)
      params.setNumFeatures(paramsMap.get("numFeatures").toString.toInt)
      params.setCheckpointInterval(paramsMap.get("checkpointInterval").toString.toInt)
      params.setK(paramsMap.get("k").toString.toInt)
      params.setMaxIter(paramsMap.get("maxIter").toString.toInt)
      params.setInputDataType(paramsMap.get("inputDataType").toString)

      var appName = s"LDA_${dataStructure}_${datasetName}_${apiName}_${cpuName}"
      if (cpuName.equals("raw")){
        appName = s"LDA_RAW_${dataStructure}_${datasetName}_${apiName}_${cpuName}"
      }
      params.setTestcaseType(appName)

      val confTrain = new SparkConf()
        .setAppName(s"LDA_${dataStructure}_${datasetName}_${apiName}_${cpuName}")
      val sparkTrain = SparkSession.builder.config(confTrain).getOrCreate()

      val (res, costTime) = dataStructure match {
        case "dataframe" => {
          val (trainModel, dataFrameCostTime) = new LDAKernel().runDataFrameJob(sparkTrain, params)
          val confPredict = new SparkConf()
            .setAppName(s"LDA_${dataStructure}_${datasetName}_${apiName}_${cpuName}_predict").set("spark.task.cpus", "1")
          val sparkPredict = SparkSession.builder.config(confPredict).getOrCreate()
          val dataFrameRes = new LDAKernel().runPredictJob(sparkPredict, params, trainModel)
          (dataFrameRes, dataFrameCostTime)
        }
        case "rdd" => (0.0, new LDAKernel().runRDDJob(sparkTrain, params))
      }

      params.setEvaluation(res)
      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/LDA_${
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
