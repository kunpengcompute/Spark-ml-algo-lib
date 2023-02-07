package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.FPGVerify

import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.fpm.FPGrowth

import java.io.FileWriter
import java.util
import java.util.Date
import scala.beans.BeanProperty

class FPGConfig extends Serializable {
  @BeanProperty var fpg: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class FPGParams extends Serializable {
  @BeanProperty var pt: Int = _
  @BeanProperty var itemsCol: String = _
  @BeanProperty var minSupport: Double = _
  @BeanProperty var minConfidence: Double = _
  @BeanProperty var optLevel: Int = _
  @BeanProperty var timeLimit1: String = _
  @BeanProperty var timeLimit2: String = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
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

object FPGRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/fpg/fpg.yml")
      val representer = new Representer
      representer.addClassTag(classOf[FPGParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[FPGConfig]), representer, options)
      val description = new TypeDescription(classOf[FPGParams])
      yaml.addTypeDescription(description)
      val configs: FPGConfig = yaml.load(stream).asInstanceOf[FPGConfig]
      val params = new FPGParams()
      val paramsMap: util.HashMap[String, Object] = configs.fpg.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      params.setPt(paramsMap.getOrDefault("pt", "276").asInstanceOf[Int])
      params.setItemsCol(paramsMap.getOrDefault("itemsCol", "items").asInstanceOf[String])
      params.setMinSupport(paramsMap.getOrDefault("minSupport", "0.1").asInstanceOf[Double])
      params.setMinConfidence(paramsMap.getOrDefault("minConfidence", "0.8").asInstanceOf[Double])
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("FPG")
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
      if (isRaw.equals("no")) {
        conf.set("spark.boostkit.ml.fpgrowth.optLevel",
          paramsMap.get("optLevel").asInstanceOf[Int].toString)
        conf.set("spark.boostkit.ml.fpgrowth.timeLimit1",
          paramsMap.get("timeLimit1").asInstanceOf[String])
        conf.set("spark.boostkit.ml.fpgrowth.timeLimit2",
          paramsMap.get("timeLimit2").asInstanceOf[String])
      }
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val costTime = new FPGKernel().runJob(spark, params)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(FPGVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
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

class FPGKernel {
  def runJob(spark: SparkSession, params: FPGParams): Double = {
    import spark.implicits._
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val dataPath = params.dataPath
    val pt = params.pt
    val itemsCol = params.itemsCol
    val minSupport = params.minSupport
    val minConfidence = params.minConfidence

    val t1 = System.currentTimeMillis()
    println("\n--------start--------\n")
    println("* start:             " + t1)

    // Load and parse the data
    val oriSequences = sc.textFile(dataPath).map { line =>
      var items = Set.empty[Int]
      line.split(" ").foreach {itemStr =>
        val item = itemStr.toInt
        if (item >= 0) {
          items += item
        }
      }
      items
    }
    val trainData = sc.parallelize(oriSequences.collect(), pt).cache()
    println("trainData: " + trainData.count())
    val t2 = System.currentTimeMillis()
    println("* after preprocess:  " + t2)

    val fpgrowth = new FPGrowth()
      .setItemsCol(itemsCol)
      .setMinSupport(minSupport)
      .setMinConfidence(minConfidence)

    val sequence = trainData.toDF(itemsCol)

    val model = fpgrowth.fit(sequence)
    model.freqItemsets.show()
    model.associationRules.show()
    val t3 = System.currentTimeMillis()
    println("* after train:       " + t3)
    println("\n--------success--------\n")
    val cnt = model.freqItemsets.count()
    print(s"freqItemsets count = $cnt\n\n")

    val totalTaining = (t3 - t1).toDouble / 1000
    val coreTraining = (t3 - t2).toDouble / 1000
    val dataLoading = (t2 - t1).toDouble / 1000
    println("[s]end2end train:   " + totalTaining)
    println("[s]data preprocess: " + dataLoading)
    println("[s]core train:      " + coreTraining)
    val df = model.freqItemsets.map(row => row.toString().sorted)
    FPGVerify.saveRes(df, params.saveDataPath, sc)
    totalTaining
  }
}