package com.bigdata.ml

import com.huawei.bigdata.alogrithms.isolationforest._
import com.linkedin.relevance.isolationforest.{IsolationForest => LinkedinIsolationForest}
import com.bigdata.utils.Utils
import com.bigdata.compare.ml.UpEvaluationVerify

import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.Row

import java.io.FileWriter
import java.util
import java.util.Date
import scala.beans.BeanProperty

class IFConfig extends Serializable {
  @BeanProperty var IF: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class IFParams extends Serializable {
  @BeanProperty var pt: Int = _
  @BeanProperty var numTrees: Int = _
  @BeanProperty var bootstrap: Boolean = _
  @BeanProperty var maxInstances: Int = _
  @BeanProperty var maxFea: Double = _
  @BeanProperty var featuresCol: String = _
  @BeanProperty var predictionCol: String = _
  @BeanProperty var scoreCol: String = _
  @BeanProperty var contamination: Double = _
  @BeanProperty var randomSeed: Int = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var auROC: Double = _
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

object IFRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/if/if.yml")
      val representer = new Representer
      representer.addClassTag(classOf[IFParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[IFConfig]), representer, options)
      val description = new TypeDescription(classOf[IFParams])
      yaml.addTypeDescription(description)
      val configs: IFConfig = yaml.load(stream).asInstanceOf[IFConfig]
      val params = new IFParams()
      val paramsMap: util.HashMap[String, Object] = configs.IF.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      params.setPt(paramsMap.getOrDefault("pt", "280").asInstanceOf[Int])
      params.setNumTrees(paramsMap.getOrDefault("numTrees", "100").asInstanceOf[Int])
      params.setBootstrap(paramsMap.getOrDefault("bootstrap", "false").asInstanceOf[Boolean])
      params.setMaxInstances(paramsMap.getOrDefault("maxInstances", "256").asInstanceOf[Int])
      params.setMaxFea(paramsMap.getOrDefault("maxFea", "1.0").asInstanceOf[Double])
      params.setFeaturesCol(paramsMap.getOrDefault("featuresCol", "features").asInstanceOf[String])
      params.setPredictionCol(paramsMap.getOrDefault("predictionCol", "predictedLabel").asInstanceOf[String])
      params.setScoreCol(paramsMap.getOrDefault("scoreCol", "anomalyScore").asInstanceOf[String])
      params.setContamination(paramsMap.getOrDefault("contamination", "0.1").asInstanceOf[Double])
      params.setRandomSeed(paramsMap.getOrDefault("randomSeed", "11").asInstanceOf[Int])
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("IF")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${datasetName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${datasetName}_raw"
        params.setVerifiedDataPath(params.saveDataPath)
        params.setSaveDataPath(s"${params.saveDataPath}_raw")
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val (auROC, costTime) = new IFKernel().runJob(spark, params)
      params.setAuROC(auROC)
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
      println(s"Exec Successful: costTime: ${costTime}s; auROC: ${auROC};isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

class IFKernel {
  def runJob(spark: SparkSession, params: IFParams): (Double, Double) = {
    import spark.implicits._
    val sc = spark.sparkContext
    val dataPath = params.dataPath
    val pt = params.pt
    val numTrees = params.numTrees
    val bootstrap = params.bootstrap
    val maxInstances = params.maxInstances
    val maxFea = params.maxFea
    val featuresCol = params.featuresCol
    val predictionCol = params.predictionCol
    val scoreCol = params.scoreCol
    val contamination = params.contamination
    val randomSeed = params.randomSeed

    val startTime = System.currentTimeMillis()
    val data0 = sc.textFile(dataPath).repartition(pt).map { line =>
      val parts = line.split(',')
      (Vectors.dense(parts(1).split(' ').map(_.toDouble)), parts(0).toDouble)
    }.cache()
    val data = data0.toDF("features", "label")
    println(data.count())

    val isolationForest = params.isRaw match {
      case "yes" => {
        val IF = new LinkedinIsolationForest()
          .setNumEstimators(numTrees)
          .setBootstrap(bootstrap)
          .setMaxSamples(maxInstances)
          .setMaxFeatures(maxFea)
          .setFeaturesCol(featuresCol)
          .setPredictionCol(predictionCol)
          .setScoreCol(scoreCol)
          .setContamination(contamination)
          .setContaminationError(0.01 * contamination)
          .setRandomSeed(randomSeed)
        IF
      }
      case "no" => {
        val IF = new IsolationForest()
          .setNumTrees(numTrees)
          .setBootstrap(bootstrap)
          .setMaxInstances(maxInstances)
          .setMaxFea(maxFea)
          .setFeaturesCol(featuresCol)
          .setPredictionCol(predictionCol)
          .setScoreCol(scoreCol)
          .setAnomalyRatio(contamination)
          .setAnomalyRatioError(0.01 * contamination)
          .setRandomSeed(randomSeed)
        IF
      }
    }

    val isolationForestModel = isolationForest.fit(data)
    val dataWithScores = isolationForestModel.transform(data).cache()
    println(dataWithScores.count())
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val predictionAndLabels = dataWithScores.select("anomalyScore", "label")
      .rdd.map {
      case Row(score: Float, label: Double) =>
        (score.toDouble, label)
      case Row(score: Double, label: Double) =>
        (score, label)
    }

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
    Utils.saveEvaluation(auROC, params.saveDataPath, sc)
    (auROC, costTime)
  }
}
