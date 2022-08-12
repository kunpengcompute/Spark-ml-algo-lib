package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.SimRankVerify

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation.{SimRank, SimRankOpenSource}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.util
import java.io.{File, FileWriter}
import java.util.HashMap
import scala.beans.BeanProperty

class SimRankConfig extends Serializable {

  @BeanProperty var simrank: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class SimRankParams extends Serializable {

  @BeanProperty var numPartitions: Int = _
  @BeanProperty var damp: Double = _
  @BeanProperty var maxIter: Int = _

  @BeanProperty var isRaw: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}
object SimRankRunner {
  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)
      val datasetCpuName = s"${datasetName}-${cpuName}"

      val stream = Utils.getStream("conf/ml/simrank/simrank.yml")
      val representer = new Representer
      representer.addClassTag(classOf[SimRankParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SimRankConfig]), representer, options)
      val description = new TypeDescription(classOf[SimRankParams])
      yaml.addTypeDescription(description)
      val config: SimRankConfig = yaml.load(stream).asInstanceOf[SimRankConfig]
      val params = new SimRankParams()
      val paramsMap = config.simrank.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetCpuName)
      params.setNumPartitions(paramsMap.get("numPartitions").asInstanceOf[Int])
      params.setMaxIter(paramsMap.get("maxIter").asInstanceOf[Int])
      params.setDamp(paramsMap.get("damp").asInstanceOf[Double])
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("SimRank")
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
      val costTime = new SimRankKernel().runJob(spark, params)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(SimRankVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
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


class SimRankKernel {

  def runJob(spark: SparkSession, params: SimRankParams): Double = {
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()

    import spark.implicits._
    val userCol = "user"
    val itemCol = "item"
    val df = spark.sparkContext.objectFile[Rating[Int]](params.getDataPath).repartition(params.getNumPartitions)
      .map(row => {
        ("user-" + row.user.toString, "item-" + row.item.toString)
      }).toDF(userCol, itemCol)

    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)
    if (params.getIsRaw.equals("no")) {
      val simrank = new SimRank()
        .setDamp(params.getDamp)
        .setNumIter(params.getMaxIter)
        .setUserCol(userCol)
        .setItemCol(itemCol)
      val simrankRes = simrank.computeSimilarity(df)
      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      SimRankVerify.saveRes(simrankRes.userSimilarity, simrankRes.itemSimilarity, params.saveDataPath, sc)

    } else {
      val simrankRes = new SimRankOpenSource().execute(df, (userCol, itemCol), params.getDamp, params.getMaxIter)
      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      SimRankVerify.saveRes(simrankRes._1, simrankRes._2, params.saveDataPath, sc)
    }
    params.getCostTime
  }
}
