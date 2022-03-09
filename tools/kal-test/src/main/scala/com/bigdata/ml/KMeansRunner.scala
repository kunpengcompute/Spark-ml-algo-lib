package com.bigdata.ml

import java.io.{File, FileWriter}
import java.util.HashMap

import com.bigdata.utils.Utils
import org.apache.hadoop.io.LongWritable
import org.apache.mahout.math.VectorWritable
import org.apache.spark.ml.clustering.{KMeans => MlKMeans}
import org.apache.spark.ml.linalg.{Vectors => MlVectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.clustering.{KMeans => MlibKMeans}
import org.apache.spark.mllib.linalg.{Vectors => MlibVectors}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class KMeansConfig extends Serializable {

  @BeanProperty var kmeans: HashMap[String, Object] = _
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

    val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
    for (i <- 0 to paramMaps.size -1) {
      paramMaps(i) = ParamMap(kmeans.k -> params.k)
        .put(kmeans.maxIter, params.maxIterations)
    }

    val model = params.apiName match {
      case "fit" => kmeans.fit(trainingData)
      case "fit1" =>
        val models = kmeans.fit(trainingData, paramMaps)
        models(0)
    }
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)
    val res = model.computeCost(trainingData)
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
    (res, costTime)
  }
}

object KMeansRunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (dataStructure, datasetName, apiName, cpuName) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))

      val dataPath = args(1)

      val datasetCpuName = s"${datasetName}_${cpuName}"
      val stream = Utils.getStream("conf/ml/kmeans/kmeans.yml")

      val representer = new Representer
      representer.addClassTag(classOf[KMeansParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[KMeansConfig]), representer, options)
      val description = new TypeDescription(classOf[KMeansParams])
      yaml.addTypeDescription(description)
      val config: KMeansConfig = yaml.load(stream).asInstanceOf[KMeansConfig]
      val paramsMap = config.kmeans.get(datasetCpuName).asInstanceOf[HashMap[String, Object]]

      val params = new KMeansParams()

      params.setApiName(apiName)
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setDatasetCpuName(datasetCpuName)
      params.setAlgorithmName("KMeans")
      if (cpuName == "raw") {
        params.setIsRaw("yes")
      }

      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)
      params.setMaxIterations(paramsMap.get("maxIterations").toString.toInt)
      params.setK(paramsMap.get("k").toString.toInt)

      var appName = s"KMeans_${dataStructure}_${datasetName}_${apiName}_${cpuName}"
      if (cpuName.equals("raw")){
        appName = s"KMeans_RAW_${dataStructure}_${datasetName}_${apiName}_${cpuName}"
      }
      params.setTestcaseType(appName)
      val conf = new SparkConf()
        .setAppName(s"KMeans_${dataStructure}_${datasetName}_${apiName}_${cpuName}")
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val (res, costTime) = dataStructure match {
        case "dataframe" => new KMeansKernel().runDataFrameJob(spark, params)
        case "rdd" => new KMeansKernel().runRDDJob(spark, params)
      }
      params.setEvaluation(res)
      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/KMeans_${
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
