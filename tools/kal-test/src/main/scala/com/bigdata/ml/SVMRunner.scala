package com.bigdata.ml
import java.io.{File, FileWriter}
import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.linalg.Vectors

import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import com.bigdata.utils.Utils
import scala.beans.BeanProperty

class SVMConfig extends Serializable {
  @BeanProperty var svm: HashMap[String, Object] = _
}

class SVMParams extends Serializable {
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var regParam: Double = _
  @BeanProperty var maxIter: Int = _
  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var datasetCpuName: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var evaluation: Double = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _

}

object SVMRunner {
  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, apiName, cpuName) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))

      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))

      val datasetCpuName = s"${datasetName}_${cpuName}"
      val stream = Utils.getStream("conf/ml/svm/svm.yml")
      val representer = new Representer
      representer.addClassTag(classOf[SVMParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SVMConfig]), representer, options)
      val description = new TypeDescription(classOf[SVMParams])
      yaml.addTypeDescription(description)
      val config: SVMConfig = yaml.load(stream).asInstanceOf[SVMConfig]
      val paramsMap = config.svm.get(datasetCpuName).asInstanceOf[HashMap[String, Object]]
      val params = new SVMParams()
      params.setApiName(apiName)
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setDatasetCpuName(datasetCpuName)
      params.setDatasetName(datasetName)
      params.setAlgorithmName("SVM")
      params.setTestcaseType(s"SVM_${datasetName}_${apiName}")
      if (cpuName == "raw") {
        params.setIsRaw("yes")
        params.setTestcaseType(s"SVM_raw_${datasetName}_${apiName}")
      }
      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)
      params.setMaxIter(paramsMap.get("maxIter").toString.toInt)
      params.setRegParam(paramsMap.get("regParam").toString.toDouble)

      val startTime = System.currentTimeMillis()
      val conf = new SparkConf()
        .setAppName(s"SVM_${datasetName}_${apiName}")
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val sc = spark.sparkContext
      import spark.implicits._

//      val startTime = System.currentTimeMillis()
      val training = sc.textFile(trainingDataPath).repartition(params.numPartitions)
      val test = sc.textFile(testDataPath).repartition(params.numPartitions)
      val parsedData0 = training.map { line =>
        val parts = line.split(',')
        (parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }
        .persist()
      val parsedData = parsedData0.toDF("label", "features")

      val svm = new LinearSVC()
        .setMaxIter(params.maxIter)
        .setRegParam(params.regParam)

      val paramMap = ParamMap(svm.maxIter -> params.maxIter)
        .put(svm.regParam, params.regParam)

      val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
      for (i <- 0 to paramMaps.size -1) {
        paramMaps(i) = ParamMap(svm.maxIter -> params.maxIter)
          .put(svm.regParam, params.regParam)
      }

      val maxIterParamPair = ParamPair(svm.maxIter, params.maxIter)
      val regParamPair = ParamPair(svm.regParam, params.regParam)
      val model = params.apiName match {
        case "fit" => svm.fit(parsedData)
        case "fit1" => svm.fit(parsedData, paramMap)
        case "fit2" =>
          val models = svm.fit(parsedData, paramMaps)
          models(0)
        case "fit3" => svm.fit(parsedData, maxIterParamPair, regParamPair)
      }
      val costTime = (System.currentTimeMillis() - startTime) / 1000.0

      val parsedTest = test.map { line =>
        val parts = line.split(',')
        (parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }
        .toDF("label","features")
        .cache()
      val res = model.transform(parsedTest)
      val evaluation = res.filter($"label"===$"prediction").count().toDouble/res.count

      params.setEvaluation(evaluation)
      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/SVM_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s; evaluation: ${evaluation}")
      sc.stop
    }catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}