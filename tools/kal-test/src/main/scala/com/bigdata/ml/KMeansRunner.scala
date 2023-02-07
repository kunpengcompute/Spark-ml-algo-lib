package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.DownEvaluationVerify

import org.apache.hadoop.io.LongWritable
import org.apache.mahout.math.VectorWritable
import org.apache.spark.ml.clustering.{KMeans => MlKMeans}
import org.apache.spark.ml.linalg.{Vectors => MlVectors}
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.mllib.clustering.{KMeans => MlibKMeans}
import org.apache.spark.mllib.linalg.{Vectors => MlibVectors}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.util
import scala.beans.BeanProperty
import java.io.{File, FileWriter}
import java.util.HashMap

class KMeansConfig extends Serializable {

  @BeanProperty var kmeans: util.HashMap[String, util.HashMap[String, Object]] = _
}

class KMeansParams extends Serializable {

  @BeanProperty var numPartitions: Int = _
  @BeanProperty var maxIterations: Int = _
  @BeanProperty var k: Int = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var datasetCpuName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var evaluation: Double = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object KMeansRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (dataStructure, datasetName, apiName, cpuName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3), modelConfSplit(4), modelConfSplit(5))
      val dataPath = args(1)
      val datasetCpuName = s"${datasetName}_${cpuName}"
      val saveResultPath = args(2)

      val stream = Utils.getStream("conf/ml/kmeans/kmeans.yml")
      val representer = new Representer
      representer.addClassTag(classOf[KMeansParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[KMeansConfig]), representer, options)
      val description = new TypeDescription(classOf[KMeansParams])
      yaml.addTypeDescription(description)
      val config: KMeansConfig = yaml.load(stream).asInstanceOf[KMeansConfig]
      val paramsMap: util.HashMap[String, Object] = config.kmeans.get(datasetCpuName)
      val params = new KMeansParams()
      params.setNumPartitions(paramsMap.get("numPartitions").asInstanceOf[Int])
      params.setMaxIterations(paramsMap.get("maxIterations").asInstanceOf[Int])
      params.setK(paramsMap.get("k").asInstanceOf[Int])
      params.setApiName(apiName)
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setDatasetCpuName(datasetCpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("KMeans")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}_${dataStructure}_${apiName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${datasetName}_${dataStructure}_${apiName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${datasetName}_${dataStructure}_${apiName}_raw"
        params.setVerifiedDataPath(params.saveDataPath)
        params.setSaveDataPath(s"${params.saveDataPath}_raw")
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName)
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val (res, costTime) = dataStructure match {
        case "dataframe" => new KMeansKernel().runDataFrameJob(spark, params)
        case "rdd" => new KMeansKernel().runRDDJob(spark, params)
      }
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

class KMeansKernel {

  def runDataFrameJob(spark: SparkSession, params: KMeansParams): (Double, Double) = {

    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()
    val data = sc.sequenceFile[LongWritable, VectorWritable](params.dataPath)
    val dataRDD = data.map{ case (k, v) =>
      var vector: Array[Double] = new Array[Double](v.get().size)
      for (i <- 0 until v.get().size) vector(i) = v.get().get(i)
      vector
    }.repartition(params.numPartitions).persist()

    import spark.implicits._
    val dataDF = dataRDD.toDF("features")
    val convertToVector = udf((array: Seq[Double]) => {
      MlVectors.dense(array.toArray)
    })
    val trainingData = dataDF.withColumn("features", convertToVector($"features"))
    println("count: " + trainingData.count())
    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    val kmeans = new MlKMeans().setK(params.k).setMaxIter(params.maxIterations)

    val paramMap = ParamMap(kmeans.k -> params.k)
      .put(kmeans.maxIter, params.maxIterations)
    val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
    for (i <- 0 to paramMaps.size -1) {
      paramMaps(i) = ParamMap(kmeans.k -> params.k)
        .put(kmeans.maxIter, params.maxIterations)
    }


    val maxIterParamPair = ParamPair(kmeans.maxIter, params.maxIterations)
    val kPair = ParamPair(kmeans.k, params.k)
    val model = params.apiName match {
      case "fit" => kmeans.fit(trainingData)
      case "fit1" => kmeans.fit(trainingData, paramMap)
      case "fit2" =>
        val models = kmeans.fit(trainingData, paramMaps)
        models(0)
      case "fit3" => kmeans.fit(trainingData, kPair, maxIterParamPair)
    }
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)
    val res = model.computeCost(trainingData)
    Utils.saveEvaluation(res, params.saveDataPath, sc)
    (res, costTime)
  }

  def runRDDJob(spark: SparkSession, params: KMeansParams): (Double, Double) = {

    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()
    val data = sc.sequenceFile[LongWritable, VectorWritable](params.dataPath)
    val dataRDD = data.map{ case (k, v) =>
      var vector: Array[Double] = new Array[Double](v.get().size)
      for (i <- 0 until v.get().size) vector(i) = v.get().get(i)
      MlibVectors.dense(vector)
    }.repartition(params.numPartitions).cache()
    println("count: " + dataRDD.count())
    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0

    val model = new MlibKMeans()
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
      .run(dataRDD)
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    params.setLoadDataTime(loadDataTime)
    val res = model.computeCost(dataRDD)
    Utils.saveEvaluation(res, params.saveDataPath, sc)
    (res, costTime)
  }
}
