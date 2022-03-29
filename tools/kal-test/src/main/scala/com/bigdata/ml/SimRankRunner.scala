//scalastyle:off
package com.bigdata.ml

import java.io.{File, FileWriter}
import java.util.HashMap

import com.bigdata.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation.{SimRank, SimRankOpenSource}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class SimRankConfig extends Serializable {

  @BeanProperty var simrank: HashMap[String, Object] = _
}

class SimRankParams extends Serializable {

  @BeanProperty var numPartitions: Int = _
  @BeanProperty var maxIter: Int = _
  @BeanProperty var damp: Double = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var savePath: String = _
  @BeanProperty var saveResult: Boolean = false
  @BeanProperty var algorithmType: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var caseName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var algorithmName: String = _
}

class SimRankKernel {

  def runJob(spark: SparkSession, params: SimRankParams): Double = {
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

    if (params.getAlgorithmType.equals("boostkit")) {
      val simrank = new SimRank()
        .setDamp(params.getDamp)
        .setNumIter(params.getMaxIter)
        .setUserCol(userCol)
        .setItemCol(itemCol)

      val simrankRes = simrank.computeSimilarity(df)
      simrankRes.itemSimilarity.foreach(_ => {})
      simrankRes.userSimilarity.foreach(_ => {})
      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)
      if(params.getSaveResult) {
        simrankRes.userSimilarity.write
          .mode("overwrite").option("header", value = true).csv(params.savePath)
        simrankRes.itemSimilarity.write
          .mode("overwrite").option("header", value = true).csv(params.savePath)
      }

    } else {
      val simrankRes = new SimRankOpenSource().execute(df, (userCol, itemCol), params.getDamp, params.getMaxIter)
      simrankRes._1.foreach(_ => {})
      simrankRes._2.foreach(_ => {})
      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)
      if(params.getSaveResult) {
        simrankRes._1.write
          .mode("overwrite").option("header", value = true).csv(params.savePath)
        simrankRes._2.write
          .mode("overwrite").option("header", value = true).csv(params.savePath)
      }
    }
    params.getCostTime
  }
}

object SimRankRunner {
  def main(args: Array[String]): Unit = {

    try {
      val caseName = args(0)
      val caseArray = caseName.split("-")
      val (datasetName, algorithmType) = (caseArray(0), caseArray(1))

      val dataPath = args(1)

      val stream = Utils.getStream("conf/ml/simrank/simrank.yml")

      val representer = new Representer
      representer.addClassTag(classOf[SimRankParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SimRankConfig]), representer, options)
      val description = new TypeDescription(classOf[SimRankParams])
      yaml.addTypeDescription(description)
      val config: SimRankConfig = yaml.load(stream).asInstanceOf[SimRankConfig]
      val paramsMap = config.simrank.get(caseName).asInstanceOf[HashMap[String, Object]]

      val params = new SimRankParams()

      params.setAlgorithmType(algorithmType)
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCaseName(caseName)

      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)
      params.setMaxIter(paramsMap.get("maxIter").toString.toInt)
      params.setDamp(paramsMap.get("damp").toString.toDouble)
      params.setSaveResult(paramsMap.get("saveResult").toString.toBoolean)
      params.setSavePath(paramsMap.get("savePath").toString)
      params.setAlgorithmName("simrank")

      val conf = new SparkConf()
        .setAppName(s"SimRank_${caseName}")
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val costTime = new SimRankKernel().runJob(spark, params)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/SimRank_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
