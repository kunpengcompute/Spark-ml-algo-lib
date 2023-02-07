package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.UpEvaluationVerify

import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.Hdbscan
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.linalg.{Vector, Vectors}

import java.io.{File, FileWriter, PrintWriter}
import java.util
import java.util.Date
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

class HDBConfig extends Serializable {
  @BeanProperty var hdb: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class HDBParams extends Serializable {
  @BeanProperty var pt: Int = _
  @BeanProperty var mstPartitionNum: Int = _
  @BeanProperty var seed: Int = _
  @BeanProperty var saurfangThreshold: Double = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var standSilhouette: Double = _
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

object HDBRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/hdb/hdb.yml")
      val representer = new Representer
      representer.addClassTag(classOf[HDBParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[HDBConfig]), representer, options)
      val description = new TypeDescription(classOf[HDBParams])
      yaml.addTypeDescription(description)
      val configs: HDBConfig = yaml.load(stream).asInstanceOf[HDBConfig]
      val paramsMap: util.HashMap[String, Object] = configs.hdb.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new HDBParams()
      params.setPt(paramsMap.get("pt").asInstanceOf[Int])
      params.setMstPartitionNum(paramsMap.get("mstPartitionNum").asInstanceOf[Int])
      params.setSeed(paramsMap.get("seed").asInstanceOf[Int])
      params.setSaurfangThreshold(paramsMap.get("saurfangThreshold").asInstanceOf[Double])
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("HDB")
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
      val (res, costTime) = new HDBKernel().runJob(spark, params)
      params.setStandSilhouette(res)
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
      println(s"Exec Successful: costTime: ${costTime}min; standSilhouette: ${res};isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

class HDBKernel {
  def runJob(spark: SparkSession, params: HDBParams): (Double, Double) = {
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val startTime = System.currentTimeMillis()

    println("\n--------start--------\n")

    //    val dataRDD = sc.textFile(dataPath).map
    //    {
    //      t=>t.split(",").map{t=>t.toDouble}
    //    }.repartition(pt).persist()

    import scala.io.{BufferedSource, Source}
    val data = new ArrayBuffer[Vector]()
    val source2: BufferedSource = Source.fromFile(params.dataPath)
    for (line <- source2.getLines()){//source.getLines()获取所有的行
      data.append(Vectors.dense(line.split(",").map{_.toDouble}))
    }

    val d1 = data.toArray
    val dataRDD = sc.parallelize(d1).repartition(params.pt).cache()

    println("count: "+ dataRDD.count())
    println("dim: " + dataRDD.first().size)
    val t1 = System.currentTimeMillis()
    println("map Cost[min]: " + (t1 - startTime).toDouble/60/1000)

    val hdb = new Hdbscan()
      .setMstPartitionNum(params.mstPartitionNum)
      .setSaurfangThreshold(params.saurfangThreshold)
      .setRandomSeed(params.seed)
    val labels = hdb.fit(dataRDD)
    val t2 = System.currentTimeMillis()
    println("train Cost[min]: " + (t2 - t1).toDouble/60/1000)
    println("total Cost[min]: " + (t2 - startTime).toDouble/60/1000)

    import spark.implicits._
    val valid = labels.map{t => (t._2, t._3)}.toDF("features", "prediction")
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(valid)
    val standSilhouette = (silhouette + 1) / 2.0
    println(s"Silhouette with squared euclidean distance = $standSilhouette")
    // labels.map{t=>(t._3,1)}.reduceByKey{(x,y)=>x+y}.collect().foreach(println)
    println("\n--------success--------\n")
    Utils.saveEvaluation(standSilhouette, params.saveDataPath, sc)
    val costTime = (t2 - startTime).toDouble/60/1000
    (standSilhouette, costTime)
  }
}