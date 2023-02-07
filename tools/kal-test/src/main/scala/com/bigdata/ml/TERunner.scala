package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.utils.TimeUtils
import com.bigdata.compare.ml.TEVerify

import ai.h2o.sparkling.{H2OConf, H2OContext}
import ai.h2o.sparkling.ml.features.H2OTargetEncoder
import ai.h2o.sparkling.ml.models.H2OTargetEncoderModel
import org.apache.spark.ml.feature.{TargetEncoder, TargetEncoderModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, DataFrame, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.FileWriter
import java.util
import java.util.Date
import scala.beans.BeanProperty


class TEConfig extends Serializable {
  @BeanProperty var te: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class TEParams extends Serializable {
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var problemType: String = _
  @BeanProperty var targetColName: String = _
  @BeanProperty var blendedAvgSmoothing: Int = _
  @BeanProperty var blendedAvgInflectionPoint: Int = _

  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var startTime: Long = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object TERunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) = {
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      }
      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/te/te.yml")
      val representer = new Representer
      representer.addClassTag(classOf[TEParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[TEConfig]), representer, options)
      val description = new TypeDescription(classOf[TEParams])
      yaml.addTypeDescription(description)
      val configs: TEConfig = yaml.load(stream).asInstanceOf[TEConfig]
      val paramsMap: util.HashMap[String, Object] = configs.te.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new TEParams()
      params.setNumPartitions(paramsMap.get("numPartitions").asInstanceOf[Int])
      params.setProblemType(paramsMap.get("problemType").asInstanceOf[String])
      params.setTargetColName(paramsMap.get("targetColName").asInstanceOf[String])
      params.setBlendedAvgSmoothing(paramsMap.get("blendedAvgSmoothing").asInstanceOf[Int])
      params.setBlendedAvgInflectionPoint(paramsMap.get("blendedAvgInflectionPoint").asInstanceOf[Int])
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("TargetEncoder")
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
      val costTime = new TEKernel().runJob(spark, params)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(TEVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
        val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
        writerIsCorrect.write(s"${params.testcaseType} ${params.isCorrect} \n")
        writerIsCorrect.close()
      }

      val writer = new FileWriter(s"report/${params.testcaseType}_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s;isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

class TEKernel {
  def runJob(spark: SparkSession, params: TEParams): Double = {
    val sc = spark.sparkContext
    val timer = new TimeUtils.Timer("main").start()
    val targetColName = params.targetColName
    val problemType = params.problemType
    val numPartitions = params.numPartitions
    val startTime = System.currentTimeMillis()

    import spark.implicits._
    val encodedTrainSavePath = s"${params.saveDataPath}/encodedTrain"
    val encodedTestSavePath = s"${params.saveDataPath}/encodedTest"
    val trainSchema = getStructTypeWithFoldCol(getSchemaByName(params.datasetName))
    var trainDF = spark.read.option("header", false)
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .schema(trainSchema)
      .csv(params.trainingDataPath)
    trainDF = trainDF.na.fill(0).na.fill("MISSING").repartition(numPartitions).cache()
    trainDF.rdd.foreachPartition(_ => {})
    println(s"trainingDF.numpartitions: ${trainDF.rdd.getNumPartitions}")
    timer.tic("[load & preprocessTrain]")

    val (catFeaturesSFs, numFeaturesSFs) = trainDF.schema
      .filter(_.name != targetColName)
      .filter(_.name != "foldCol")
      .partition(_.dataType.toString == "StringType")
    val catFeatures = catFeaturesSFs.map(_.name).toArray
    val numFeatures = numFeaturesSFs.map(_.name).toArray
    val featureCols = (numFeatures ++ catFeatures.map(x => s"${x}_te")).map(col(_))

    showCardinality(catFeatures, trainDF)
    if (params.isRaw == "yes") {
      val conf = new H2OConf().setLogLevel("WARN")
      H2OContext.getOrCreate(conf)
    }

    timer.tic("[init]")
    timer.report()
    var encoderModel: Any = null
    val encodedTrain = if (params.isRaw == "yes") {
      var encoder = new H2OTargetEncoder()
        .setInputCols(catFeatures)
        .setHoldoutStrategy("KFold")
        .setProblemType(problemType)
        .setFoldCol("foldCol")
        .setLabelCol(targetColName)
        .setNoise(0.0)
        .setBlendedAvgEnabled(true)
        .setBlendedAvgSmoothing(params.blendedAvgSmoothing)
        .setBlendedAvgInflectionPoint(params.blendedAvgInflectionPoint)
        .fit(trainDF)
      timer.tic("[fit]")
      encoderModel = encoder
      encoder.transformTrainingDataset(trainDF).select((featureCols :+ col(targetColName)): _ * )
    } else {
      var encoder = new TargetEncoder()
        .setInputCols(catFeatures)
        .setProblemType(problemType)
        .setFoldCol("foldCol")
        .setLabelCol(targetColName)
        .setBlendedAvgSmoothing(20)
        .setBlendedAvgInflectionPoint(10)
        .fit(trainDF)
      timer.tic("[fit]")
      encoderModel = encoder
      encoder.transformTrainingDataset(trainDF).select((featureCols :+ col(targetColName)): _ * )
    }
    encodedTrain.write.mode(SaveMode.Overwrite).parquet(encodedTrainSavePath)
    timer.tic("[transformTrain]")
    timer.report()

    println("Transforming test dataset for functionality test. This is not included in training time.")
    val testSchema = getSchemaByName(params.datasetName)
    var testDF = spark.read.option("header", false)
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .schema(testSchema)
      .csv(params.testDataPath)
    testDF = testDF.na.fill(0).na.fill("MISSING").repartition(numPartitions).cache()
    testDF.rdd.foreachPartition(_ => {})
    timer.tic("[load & preprocessTest]")
    val encodedTest = if (params.isRaw == "yes") {
      val encoder = encoderModel.asInstanceOf[H2OTargetEncoderModel]
      val encodedTest = encoder.transform(testDF).select((featureCols :+ col(targetColName)): _ * )
      timer.tic("[transformTest]")
      encodedTest
    } else {
      val encoder = encoderModel.asInstanceOf[TargetEncoderModel]
      val encodedTest = encoder.transform(testDF).select((featureCols :+ col(targetColName)): _ * )
      timer.tic("[transformTest]")
      encodedTest
    }
    encodedTest.write.mode(SaveMode.Overwrite).parquet(encodedTestSavePath)
    val costTime = (System.currentTimeMillis() - startTime) / 1000
    println("Transform test complete.")
    costTime
  }

  def showCardinality(catFeatures: Array[String], trainingDF: Dataset[_]): Unit = {
    val cards = catFeatures.map { catF =>
      countDistinct(col(catF))
    }
    val totalCardinality = trainingDF.select(cards: _ * ).first().toSeq.map(_.asInstanceOf[Long]).sum
    println(s"Number of instances: ${trainingDF.count()}, Total cardinalities: ${totalCardinality}.")
    trainingDF.show(10)
  }

  def getSchemaByName(name: String): StructType = {
    val lowerCaseName = name.toLowerCase()
    if (lowerCaseName.startsWith("criteo")) {
      Encoders.product[Criteo].schema
    } else if (lowerCaseName == "click") {
      Encoders.product[Click].schema
    } else if (lowerCaseName == "taobao") {
      Encoders.product[Taobao].schema
    } else if (lowerCaseName == "movielens") {
      Encoders.product[MovieLens].schema
    } else if (lowerCaseName == "adult") {
      Encoders.product[Adult].schema
    } else null
  }

  def getStructTypeWithFoldCol(tpe: StructType): StructType = {
    StructType(tpe.toArray :+ StructField("foldCol", IntegerType, false))
  }

}

case class Adult(
                  age: Option[Double],
                  workclass: Option[String],
                  fnlwgt: Option[Double],
                  education: Option[String],
                  education_num: Option[Double],
                  marital_status: Option[String],
                  occupation: Option[String],
                  relationship: Option[String],
                  race: Option[String],
                  sex: Option[String],
                  capitalgain: Option[Double],
                  capitalloss: Option[Double],
                  hoursperweek: Option[Double],
                  native_country: Option[String],
                  target: Double)

case class Click(
                  target: Double,
                  var1: Option[String],
                  var2: Option[String],
                  var3: Option[String],
                  var4: Option[String],
                  var5: Option[Double],
                  var6: Option[Double],
                  var7: Option[String],
                  var8: Option[String],
                  var9: Option[String],
                  var10: Option[String],
                  var11: Option[String])

case class Criteo(
                   target: Double,
                   var1: Option[Double],
                   var2: Option[Double],
                   var3: Option[Double],
                   var4: Option[Double],
                   var5: Option[Double],
                   var6: Option[Double],
                   var7: Option[Double],
                   var8: Option[Double],
                   var9: Option[Double],
                   var10: Option[Double],
                   var11: Option[Double],
                   var12: Option[Double],
                   var13: Option[Double],
                   var14: Option[String],
                   var15: Option[String],
                   var16: Option[String],
                   var17: Option[String],
                   var18: Option[String],
                   var19: Option[String],
                   var20: Option[String],
                   var21: Option[String],
                   var22: Option[String],
                   var23: Option[String],
                   var24: Option[String],
                   var25: Option[String],
                   var26: Option[String],
                   var27: Option[String],
                   var28: Option[String],
                   var29: Option[String],
                   var30: Option[String],
                   var31: Option[String],
                   var32: Option[String],
                   var33: Option[String],
                   var34: Option[String],
                   var35: Option[String],
                   var36: Option[String],
                   var37: Option[String],
                   var38: Option[String],
                   var39: Option[String])

case class Taobao(
                   userID: Option[String],
                   itemID: Option[String],
                   categoryID: Option[String],
                   target: Double)

case class MovieLens(
                      userID: Option[String],
                      movieID: Option[String],
                      target: Double)