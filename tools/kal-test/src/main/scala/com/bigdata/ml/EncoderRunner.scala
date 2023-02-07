package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.EncoderVerify

import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.FeatureEncoding
import org.apache.spark.ml.feature.FeatureEncodingOrigin

import java.io.FileWriter
import java.util
import java.util.Date
import scala.beans.BeanProperty


class EncoderConfig extends Serializable {
  @BeanProperty var encoder: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class EncoderParams extends Serializable {
  @BeanProperty var encodeColumns: String = _
  @BeanProperty var numThread: Int = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var mapLoadPath: String = _
  @BeanProperty var localSavePath: String = _
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

object EncoderRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val pathSplit = args(1).split(",")
      val (dataPath, mapLoadPath, localSavePath) =
        (pathSplit(0), pathSplit(1), pathSplit(2))
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/encoder/encoder.yml")
      val representer = new Representer
      representer.addClassTag(classOf[EncoderParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[EncoderConfig]), representer, options)
      val description = new TypeDescription(classOf[EncoderParams])
      yaml.addTypeDescription(description)
      val configs: EncoderConfig = yaml.load(stream).asInstanceOf[EncoderConfig]
      val paramsMap: util.HashMap[String, Object] = configs.encoder.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new EncoderParams()
      params.setEncodeColumns(paramsMap.get("encodeColumns").asInstanceOf[String])
      params.setNumThread(paramsMap.get("numThread").asInstanceOf[Int])
      params.setDataPath(dataPath)
      params.setMapLoadPath(mapLoadPath)
      params.setLocalSavePath(localSavePath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("Encoder")
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
      val costTime = new EncoderKernel().runJob(spark, params)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(EncoderVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
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

class EncoderKernel {
  def runJob(spark: SparkSession, params: EncoderParams): Double = {
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()

    import spark.implicits._
    val df = spark.sparkContext.textFile(params.dataPath).map{
      t =>
        val row = t.split(",")
        (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9),
          row(10), row(11), row(12), row(13), row(14))
    }.toDF("1xxx","2xxx","3xxx","4xxx","5xxx","6xxx","7xxx","8xxx","9xxx","10xxx","11xxx","12xxx","13xxx","14xxx","15xxx")
      .repartition(800)
      .cache()

    val encoder = params.isRaw match {
      case "yes" => {
        val fe = new FeatureEncodingOrigin()
          .setMapLoadPath(params.mapLoadPath)
          .setDataPath(params.dataPath)
          .setOutputFilePath(params.saveDataPath)
          .setLocalSavePath(params.localSavePath)
          .setEncodeColumns(params.encodeColumns)
          .setNumThread(params.numThread)
        fe.execute(df)
        fe
      }
      case "no" => {
        val fe = new FeatureEncoding()
          .setMapLoadPath(params.mapLoadPath)
          .setDataPath(params.dataPath)
          .setOutputFilePath(params.saveDataPath)
          .setLocalSavePath(params.localSavePath)
          .setEncodeColumns(params.encodeColumns)
          .setNumThread(params.numThread)
        fe.execute(df)
        fe
      }
    }

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    costTime
  }
}