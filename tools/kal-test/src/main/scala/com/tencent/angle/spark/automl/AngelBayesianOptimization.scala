// scalastyle:off
package com.tencent.angel.spark.automl

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.UpEvaluationVerify

import com.tencent.angel.spark.automl.tuner.config.Configuration
import com.tencent.angel.spark.automl.tuner.parameter.ParamSpace
import com.tencent.angel.spark.automl.tuner.solver.Solver
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{LabeledPoint, OneHotEncoder, SQLTransformer, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.{Model, Pipeline}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{abs, coalesce, col, hash, lit, mean}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.ml.linalg.Vectors
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.FileWriter
import java.util
import scala.beans.BeanProperty
import scala.util.Random

class ABOConfig extends Serializable {
  @BeanProperty var bo: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class ABOParams extends Serializable {
  @BeanProperty var partitionNum: Int = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var evaluation: Double = _
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

object AngelBayesianOptimization {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/bo/bo.yml")
      val representer = new Representer
      representer.addClassTag(classOf[ABOParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[ABOConfig]), representer, options)
      val description = new TypeDescription(classOf[ABOParams])
      yaml.addTypeDescription(description)
      val configs: ABOConfig = yaml.load(stream).asInstanceOf[ABOConfig]
      val paramsMap: util.HashMap[String, Object] = configs.bo.get("raw").get(datasetName)
      val params = new ABOParams()
      params.setPartitionNum(paramsMap.getOrDefault("partitionNum", "1000").asInstanceOf[Int])
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("BO")
      params.setVerifiedDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}")
      params.setSaveDataPath(s"${params.verifiedDataPath}_raw")
      var appName = s"${params.algorithmName}_${datasetName}_raw"
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      println(s"Initialized spark session.")
      val startTime = System.currentTimeMillis()
      params.setStartTime(startTime)
      val sc = spark.sparkContext

      var searchArray:Array[Int] = Array()
      var metricArray:Array[Double] = Array()
      var timeArray:Array[Double] = Array()
      for (a <- 1 to 10) {
        val res = if (datasetName == "BostonHousing") {
          bostonHousingRfRegressor(spark, params, a.toLong)
        } else if (datasetName == "TitanicRf") {
          titanicRf(spark, params, a.toLong)
        } else if (datasetName == "TitanicGBT") {
          titanicGBT(spark, params, a.toLong)
        } else {
          (0, 0.0, 0.0)
        }
        searchArray +:= res._1
        metricArray +:= res._2
        timeArray +:= res._3
      }

      val res = metricArray.sum * 1.0 / metricArray.length
      val costTime = timeArray.sum * 1.0 / metricArray.length
      println(searchArray.mkString(" "))
      println(searchArray.sum * 1.0 / metricArray.length)
      println(metricArray.mkString(" "))
      println(res)
      println(timeArray.mkString(" "))
      println(costTime)
      params.setEvaluation(res)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if (ifCheck.equals("yes")) {
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

      println(s"Exec Successful: costTime: ${costTime}s; evaluation: ${res};isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }

  def bostonHousingRfRegressor(spark: SparkSession, params: ABOParams, seed:Long): (Int, Double, Double) ={
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val trainPath = params.dataPath
    val partitionNum = params.partitionNum
    val startTime = System.currentTimeMillis()

    val housingData = spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", true).option("inferSchema", "true").csv(trainPath).repartition(partitionNum)
    val features = housingData.drop("MEDV")

    val Array(trainingData, testData) = housingData.withColumnRenamed("MEDV", "label")
      .randomSplit(Array(0.8, 0.2), seed = 42)
    trainingData.persist()
    testData.persist()
    val regressor = new RandomForestRegressor()

    var paramList: Array[ParamSpace[Double]] = Array()
    paramList :+= ParamSpace.fromConfigString(regressor.numTrees.toString(), "{3:30:1}", "Int")
    paramList :+= ParamSpace.fromConfigString(regressor.maxDepth.toString(), "{2:20:1}", "Int")
    paramList :+= ParamSpace.fromConfigString(regressor.subsamplingRate.toString(), "[0.5,1.0]", "Double")
    paramList :+= ParamSpace.fromConfigString(regressor.minInfoGain.toString(), "[0,1.0]", "Double")
    paramList :+= ParamSpace.fromConfigString(regressor.minInstancesPerNode.toString(), "{1,2,3,5,10,15,20}", "Int")

    val featureColumnsNames = features.columns.toArray
    val assembler = new VectorAssembler()
      .setInputCols(featureColumnsNames)
      .setOutputCol("features")

    val pipeline = new Pipeline().setStages(Array(assembler, regressor))
    import org.apache.spark.ml.evaluation.RegressionEvaluator

    val cv = new AngelOptCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator().setMetricName("rmse"))
      .setEstimatorParamSpace(paramList)
      //        .setSurrogate("RandomForest")
      .setSurrogate("GaussianProcess")
      .setNumIterations(500)
      .setNumFolds(5)
      .setParallelism(10)
      .setThreshold(3.45)
      .setSeed(seed)

    val model = cv.fit(trainingData)
    println(cv.searchNumber)
    println(cv.bestMetric)
    trainingData.unpersist()
    testData.unpersist()
    val endTime = System.currentTimeMillis()
    println((endTime - startTime)/1000.0)
    (cv.searchNumber, cv.bestMetric, (endTime - startTime)/1000.0 )
  }

  def titanicRf(spark: SparkSession, params: ABOParams, seed:Long): (Int, Double, Double) ={
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val trainPath = params.dataPath
    val partitionNum = params.partitionNum
    val startTime = System.currentTimeMillis()

    var dataWithNulls = {
      spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").option("header", true).csv(trainPath)
        .repartition(1)
        .withColumn("survived", col("Survived").cast(DoubleType))
        .withColumn("age", col("Age").cast(DoubleType))
        .withColumn("siblings_spouses", col("SibSp").cast(DoubleType))
        .withColumn("parents_children", col("Parch").cast(DoubleType))
        .withColumn("fare", col("Fare").cast(DoubleType))
        .select(col("survived"), col("Name") as "passenger_name", col("Pclass") as "passenger_class", col("Sex") as "sex",
          col("age"), col("fare"), col("siblings_spouses"), col("parents_children"))
        .repartition(partitionNum)
    }
    val meanAge = dataWithNulls.select(mean("age")).first.getDouble(0)
    val data = dataWithNulls.withColumn("age", coalesce(col("age"), lit(meanAge))).cache()
    val titleTransformer = new SQLTransformer("title").setStatement(
      s"""
         |SELECT *
         |, CASE WHEN passenger_name LIKE '%\\.%' THEN split(passenger_name, '\\\\.')[0]
         |       ELSE 'Nothing'
         |  END AS passenger_title
         |FROM __THIS__
      """.stripMargin
    )
    val categoricalCols = Array("passenger_class", "sex", "passenger_title")
    val indexCols = categoricalCols.map(_ + "_index")
    val oheCols = categoricalCols.map(_ + "_ohe")
    val stringIndexers = categoricalCols.map(cc => {
      new StringIndexer(s"string_indexer_$cc")
        .setHandleInvalid("keep")
        .setInputCol(cc)
        .setOutputCol(cc + "_index")
    })
    val oneHotEncoder = {
      new OneHotEncoder("ohe")
        .setHandleInvalid("keep")
        .setDropLast(false)
        .setInputCols(indexCols)
        .setOutputCols(oheCols)
    }

    val numericalCols = Array("age", "fare", "siblings_spouses", "parents_children")
    val vectorAssembler = {
      new VectorAssembler("vector_assembler")
        .setInputCols(oheCols ++ numericalCols)
        .setOutputCol("features")
    }

    val rawClassifier = new RandomForestClassifier("rf")
      .setFeaturesCol("features")
      .setLabelCol("survived")
      .setProbabilityCol("survival_prob")
      .setRawPredictionCol("survival_raw_pred")

    val pipeline = new Pipeline("pipeline")
      .setStages(Array(titleTransformer) ++ stringIndexers ++ Array(oneHotEncoder, vectorAssembler, rawClassifier))

    var paramList: Array[ParamSpace[Double]] = Array()
    paramList :+= ParamSpace.fromConfigString(rawClassifier.maxDepth.toString(), "{2:20:1}", "Int")
    paramList :+= ParamSpace.fromConfigString(rawClassifier.numTrees.toString(), "{3:30:1}", "Int")
    paramList :+= ParamSpace.fromConfigString(rawClassifier.minInfoGain.toString(), "[0,0.1]", "Double")
    paramList :+= ParamSpace.fromConfigString(rawClassifier.subsamplingRate.toString(), "[0.6,1]", "Double")

    val cv = new AngelOptCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator()
        .setLabelCol("survived")
        .setRawPredictionCol("survival_raw_pred"))
      .setEstimatorParamSpace(paramList)
      .setSurrogate("RandomForest")
      .setNumIterations(500)
      .setThreshold(0.856)
      .setNumFolds(5)
      .setSeed(seed)

    val model = cv.fit(data)
    println(cv.searchNumber)
    println(cv.bestMetric)
    data.unpersist()
    val endTime = System.currentTimeMillis()
    println((endTime - startTime)/1000.0)
    (cv.searchNumber, cv.bestMetric, (endTime - startTime)/1000.0 )
  }

  def titanicGBT(spark: SparkSession, params: ABOParams, seed:Long): (Int, Double, Double) ={
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val trainPath = params.dataPath
    val partitionNum = params.partitionNum
    val startTime = System.currentTimeMillis()

    var dataWithNulls = {
      spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").option("header", true).csv(trainPath)
        .repartition(1)
        .withColumn("survived", col("Survived").cast(DoubleType))
        .withColumn("age", col("Age").cast(DoubleType))
        .withColumn("siblings_spouses", col("SibSp").cast(DoubleType))
        .withColumn("parents_children", col("Parch").cast(DoubleType))
        .withColumn("fare", col("Fare").cast(DoubleType))
        .select(col("survived"), col("Name") as "passenger_name", col("Pclass") as "passenger_class", col("Sex") as "sex",
          col("age"), col("fare"), col("siblings_spouses"), col("parents_children"))
        .repartition(partitionNum)
    }
    val meanAge = dataWithNulls.select(mean("age")).first.getDouble(0)
    val data = dataWithNulls.withColumn("age", coalesce(col("age"), lit(meanAge))).cache()
    val titleTransformer = new SQLTransformer("title").setStatement(
      s"""
         |SELECT *
         |, CASE WHEN passenger_name LIKE '%\\.%' THEN split(passenger_name, '\\\\.')[0]
         |       ELSE 'Nothing'
         |  END AS passenger_title
         |FROM __THIS__
      """.stripMargin
    )
    val categoricalCols = Array("passenger_class", "sex", "passenger_title")
    val indexCols = categoricalCols.map(_ + "_index")
    val oheCols = categoricalCols.map(_ + "_ohe")
    val stringIndexers = categoricalCols.map(cc => {
      new StringIndexer(s"string_indexer_$cc")
        .setHandleInvalid("keep")
        .setInputCol(cc)
        .setOutputCol(cc + "_index")
    })
    val oneHotEncoder = {
      new OneHotEncoder("ohe")
        .setHandleInvalid("keep")
        .setDropLast(false)
        .setInputCols(indexCols)
        .setOutputCols(oheCols)
    }

    val numericalCols = Array("age", "fare", "siblings_spouses", "parents_children")
    val vectorAssembler = {
      new VectorAssembler("vector_assembler")
        .setInputCols(oheCols ++ numericalCols)
        .setOutputCol("features")
    }

    val rawClassifier = new GBTClassifier()
      .setFeaturesCol("features")
      .setLabelCol("survived")
      .setProbabilityCol("survival_prob")
      .setRawPredictionCol("survival_raw_pred")

    val pipeline = new Pipeline("pipeline")
      .setStages(Array(titleTransformer) ++ stringIndexers ++ Array(oneHotEncoder, vectorAssembler, rawClassifier))

    var paramList: Array[ParamSpace[Double]] = Array()
    paramList :+= ParamSpace.fromConfigString(rawClassifier.maxIter.toString(), "{3:10:1}", "Int")
    paramList :+= ParamSpace.fromConfigString(rawClassifier.subsamplingRate.toString(), "[0.5,1.0]", "Double")
    paramList :+= ParamSpace.fromConfigString(rawClassifier.minInfoGain.toString(), "[0.0,0.5]", "Double")
    paramList :+= ParamSpace.fromConfigString(rawClassifier.maxDepth.toString(), "{3:10:1}", "Int")

    val cv = new AngelOptCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator()
        .setLabelCol("survived")
        .setRawPredictionCol("survival_raw_pred"))
      .setEstimatorParamSpace(paramList)
      .setSurrogate("GaussianProcess")
      .setNumIterations(500)
      .setThreshold(0.86)
      .setNumFolds(4)
      .setSeed(seed)

    val model = cv.fit(data)
    println(cv.searchNumber)
    println(cv.bestMetric)
    data.unpersist()
    val endTime = System.currentTimeMillis()
    println((endTime - startTime)/1000.0)
    (cv.searchNumber, cv.bestMetric, (endTime - startTime)/1000.0 )
  }
}
