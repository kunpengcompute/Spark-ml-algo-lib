package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.IDFVerify

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{IDF, IDFModel}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileInputStream, FileOutputStream, FileWriter, ObjectInputStream, ObjectOutputStream}
import java.util
import scala.beans.BeanProperty

class IDFConfig extends Serializable {
  @BeanProperty var idf: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class IDFParams extends Serializable {
  @BeanProperty var pt: Int = _
  @BeanProperty var combineStrategy: String = _
  @BeanProperty var fetchMethod: String = _
  @BeanProperty var orcFormat: Boolean = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}


object IDFRunner{
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/idf/idf.yml")
      val representer = new Representer
      representer.addClassTag(classOf[IDFParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[IDFConfig]), representer, options)
      val description = new TypeDescription(classOf[IDFParams])
      yaml.addTypeDescription(description)
      val configs: IDFConfig = yaml.load(stream).asInstanceOf[IDFConfig]
      val paramsMap: util.HashMap[String, Object] = configs.idf.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new IDFParams()
      params.setPt(paramsMap.get("pt").asInstanceOf[Int])
      params.setCombineStrategy(paramsMap.get("combineStrategy").asInstanceOf[String])
      params.setFetchMethod(paramsMap.get("fetchMethod").asInstanceOf[String])
      params.setOrcFormat(paramsMap.get("orcFormat").asInstanceOf[Boolean])
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("IDF")
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
      conf.set("spark.driver.maxResultSize", "256G")
      if (isRaw.equals("no")){
        conf.set("spark.boostkit.ml.idf.combineStrategy",
          paramsMap.get("combineStrategy").asInstanceOf[String])
        conf.set("spark.boostkit.ml.idf.fetchMethod",
          paramsMap.get("fetchMethod").asInstanceOf[String])
      }
      val spark = SparkSession.builder().config(conf).getOrCreate()
      val costTime = new IDFKernel().runJob(spark, params)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(IDFVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
        val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
        writerIsCorrect.write(s"${params.testcaseType} ${params.isCorrect} \n")
        writerIsCorrect.close()
      }

      val writer = new FileWriter(s"report/${params.testcaseType}_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime};isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }

}

class IDFKernel {
  def runJob(spark: SparkSession, params: IDFParams): Double = {
    val sc = spark.sparkContext
    val startTime = System.currentTimeMillis()
    val orcData = spark.read.schema(Encoders.product[DocSchema].schema).format("orc").load(params.dataPath)
    val data = if (params.pt > 0){
      orcData.select("tf").repartition(params.pt)
    } else {
      orcData.select("tf")
    }
    val idf = new IDF().setInputCol("tf").setOutputCol("tf_idf")
    val model = idf.fit(data)
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    val res = model.idf.toArray
    IDFVerify.saveRes(res, params.saveDataPath, sc)
    costTime
  }
  case class DocSchema(id: Long, tf: Vector)
}
