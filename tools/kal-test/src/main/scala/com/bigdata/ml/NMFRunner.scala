package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.DownEvaluationVerify

import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.recommendation.{ALS, NMF}
import org.apache.spark.ml.recommendation.ALS.{Rating => ALSRating}
import org.apache.spark.ml.recommendation.NMF.{Rating => NMFRating}
import org.apache.spark.ml.evaluation.RegressionEvaluator

import java.io.{File, FileWriter, PrintWriter}
import java.util
import java.util.Date
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

class NMFConfig extends Serializable {
  @BeanProperty var nmf: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class NMFParams extends Serializable {
  @BeanProperty var userCol: String = _
  @BeanProperty var itemCol: String = _
  @BeanProperty var ratingCol: String = _
  @BeanProperty var pt: Int = _
  @BeanProperty var rank: Int = _
  @BeanProperty var maxIter: Int = _


  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var standSilhouette: Double = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var startTime: Long = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var evaluation: Double = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object NMFRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/nmf/nmf.yml")
      val representer = new Representer
      representer.addClassTag(classOf[NMFParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[NMFConfig]), representer, options)
      val description = new TypeDescription(classOf[NMFParams])
      yaml.addTypeDescription(description)
      val configs: NMFConfig = yaml.load(stream).asInstanceOf[NMFConfig]
      val paramsMap: util.HashMap[String, Object] = configs.nmf.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new NMFParams()
      params.setUserCol(paramsMap.get("userCol").asInstanceOf[String])
      params.setItemCol(paramsMap.get("itemCol").asInstanceOf[String])
      params.setRatingCol(paramsMap.get("ratingCol").asInstanceOf[String])
      params.setPt(paramsMap.get("pt").asInstanceOf[Int])
      params.setRank(paramsMap.get("rank").asInstanceOf[Int])
      params.setMaxIter(paramsMap.get("maxIter").asInstanceOf[Int])
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("NMF")
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
      spark.sparkContext.setLogLevel("ERROR")
      val (res, costTime) = new NMFKernel().runJob(spark, params)
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

class NMFKernel {
  def runJob(spark: SparkSession, params: NMFParams): (Double, Double) = {
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()

    val ratings = params.datasetName match {
      case "HibenchRating50mx10mx500m" => readHibench(spark, params.dataPath, params.pt, params.isRaw)
      case "CSJ" | "MT" | "Books" => readCSJAndMTAndBooks(spark, params.dataPath, params.pt, params.isRaw)
    }
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2), 0L)
    training.rdd.setName(s"$training").cache().count()
    test.rdd.setName(s"$test").cache().count()

    val model = params.isRaw match {
      case "no" =>{
        val nmf = new NMF()
          .setUserCol(params.userCol)
          .setItemCol(params.itemCol)
          .setRatingCol(params.ratingCol)
          .setNumBlocks(params.pt)
          .setMaxIter(params.maxIter)
          .setRank(params.rank)
        val model = nmf.fit(training)
        model.userFactors.cache().foreach(_ => {})
        model.itemFactors.cache().foreach(_ => {})
        model.setColdStartStrategy("drop")
        model
      }
      case "yes" =>{
        val alsNN = new ALS()
          .setNonnegative(true)
          .setUserCol(params.userCol)
          .setItemCol(params.itemCol)
          .setRatingCol(params.ratingCol)
          .setNumBlocks(params.pt)
          .setMaxIter(params.maxIter)
          .setRank(params.rank)
        val model = alsNN.fit(training)
        model.userFactors.cache().foreach(_ => {})
        model.itemFactors.cache().foreach(_ => {})
        model.setColdStartStrategy("drop")
        model
      }
    }
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val predictions = model.transform(test)
    val rmse = evaluator.evaluate(predictions)
    Utils.saveEvaluation(rmse, params.saveDataPath, sc)
    (rmse, costTime)
  }

  def readCSJAndMTAndBooks(
                            spark: SparkSession,
                            dataPath: String,
                            pt: Int = 250,
                            isRaw: String,
                            sep: String = ","): DataFrame = {
    import spark.implicits._
    val ratings = spark.sparkContext
      .textFile(dataPath, pt)
      .map(s => {
        s.split(sep)
      })
    val users = ratings.map(s => s(0)).distinct().zipWithIndex()
    val items = ratings.map(s => s(1)).distinct().zipWithIndex()
    if (isRaw.equals("no")) {
      ratings.map(s => (s(0), s.slice(1, 4)))
        .join(users).map { case (userName, (arr, userId)) => (arr(0), (arr.slice(1, 3), userId)) }
        .join(items).map { case (itemName, ((arr, userId), itemId)) =>
        NMFRating(userId, itemId, arr(0).toFloat)
      }.toDF()
    } else {
      ratings.map(s => (s(0), s.slice(1, 4)))
        .join(users).map { case (userName, (arr, userId)) => (arr(0), (arr.slice(1, 3), userId)) }
        .join(items).map { case (itemName, ((arr, userId), itemId)) =>
        ALSRating(userId, itemId, arr(0).toFloat)
      }.toDF()
    }
  }

  def readHibench(
                   spark: SparkSession,
                   dataPath: String,
                   pt: Int = 250,
                   isRaw: String): DataFrame = {
    import spark.implicits._
    if(isRaw.equals("no")) {
      spark.sparkContext.objectFile[ALSRating[Int]](dataPath, pt)
        .map(x => NMFRating(x.user, x.item, x.rating)).toDF()
    } else {
      spark.sparkContext.objectFile[ALSRating[Int]](dataPath, pt).toDF()
    }
  }
}