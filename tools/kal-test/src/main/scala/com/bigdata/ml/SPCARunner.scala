package com.bigdata.ml

import java.io.{File, FileWriter}
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.SPCA
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.storage.StorageLevel
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class SPCAConfig extends Serializable {

  @BeanProperty var spca: util.HashMap[String, util.HashMap[String, Object]] = _
}

class SPCAParams extends Serializable {

  @BeanProperty var pt: Int = _
  @BeanProperty var k: Int = _
  @BeanProperty var sep: String = _
  @BeanProperty var numCols: Int = _
  @BeanProperty var pcPath: String = _
  @BeanProperty var sigmaPath: String = _
  @BeanProperty var saveRes: Boolean = _

  @BeanProperty var inputDataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object SPCARunner {
  def main(args: Array[String]): Unit = {

    try {
      val datasetName = args(0)

      val inputDataPath = args(1)

      val cpuName = args(2)
      val isRaw = args(3)
      val sparkConfSplit = args(4).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))

      val stream = isRaw match {
        case "no" =>
          Utils.getStream("conf/ml/spca/spca.yml")
        case "yes" =>
          Utils.getStream("conf/ml/spca/spca_raw.yml")
      }
      val representer = new Representer
      representer.addClassTag(classOf[SPCAParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SPCAConfig]), representer, options)
      val description = new TypeDescription(classOf[SPCAParams])
      yaml.addTypeDescription(description)
      val configs: SPCAConfig = yaml.load(stream).asInstanceOf[SPCAConfig]
      val params = new SPCAParams()

      val spcaParamMap: util.HashMap[String, Object] = configs.spca.get(datasetName)
      params.setPt(spcaParamMap.getOrDefault("pt", "250").asInstanceOf[Int])
      params.setK(spcaParamMap.getOrDefault("k", "10").asInstanceOf[Int])
      params.setSep(spcaParamMap.getOrDefault("sep", " ").asInstanceOf[String])
      params.setNumCols(spcaParamMap.getOrDefault("numCols", "0").asInstanceOf[Int])
      params.setPcPath(spcaParamMap.getOrDefault("pcPath", null.asInstanceOf[String]).asInstanceOf[String])
      params.setSigmaPath(spcaParamMap.getOrDefault("sigmaPath", null.asInstanceOf[String]).asInstanceOf[String])
      params.setSaveRes(spcaParamMap.getOrDefault("saveRes", "false").asInstanceOf[Boolean])

      params.setInputDataPath(inputDataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("SPCA")

      var appName = s"SPCA_${datasetName}"
      if (isRaw == "yes") {
        appName = s"SPCA_${datasetName}_raw"
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf()
        .setAppName(appName).setMaster(master)

      val commonParas = Array (
        ("spark.submit.deployMode", deployMode),
        ("spark.executor.instances", numExec),
        ("spark.executor.cores", execCores),
        ("spark.executor.memory", execMem)
      )
      conf.setAll(commonParas)
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val costTime = new SPCAKernel().runJob(spark, params)
      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/SPCA_${
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

class SPCAKernel {

  def runJob(spark: SparkSession, params: SPCAParams): Double = {

    import spark.implicits._
    val startTime = System.currentTimeMillis()
    val trainingData = if (params.isRaw == "yes"){
      val numColsBC = spark.sparkContext.broadcast(params.numCols)
      val sepBC = spark.sparkContext.broadcast(params.sep)
      val data = spark.createDataFrame(spark.sparkContext.textFile(params.inputDataPath, params.pt)
        .map(line => {
          val entry = line.split(sepBC.value)
          (entry(0).toInt, (entry(1).toInt, entry(2).toDouble))
        }).groupByKey()
        .map { case (_, vectorEntries) => Vectors.sparse(numColsBC.value, vectorEntries.toSeq) }
        .repartition(params.pt)
        .map(Tuple1.apply))
        .toDF("matrix").persist(StorageLevel.MEMORY_ONLY)
      data
    } else {
      val data = spark.createDataFrame(spark.sparkContext.textFile(params.inputDataPath, params.pt)
        .map(line => {
          val entry = line.split(params.sep)
          (entry(0).toInt, (entry(1).toInt, entry(2).toDouble))
        }).groupByKey()
        .map{case (_, vectorEntries) => Vectors.sparse(params.numCols, vectorEntries.toSeq)}
        .repartition(params.pt)
        .map(Tuple1.apply))
        .toDF("matrix")
        .persist(StorageLevel.MEMORY_ONLY)
      data
    }


    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)

    val model = if (params.isRaw == "no"){
      new SPCA()
        .setK(params.k)
        .setInputCol("matrix")
        .fit(trainingData)
    } else {
      new PCA()
        .setK(params.k)
        .setInputCol("matrix")
        .fit(trainingData)
    }

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(costTime)

    // save result
    if (params.saveRes && params.pcPath != null && params.sigmaPath != null) {
      Utils.writeMatrix(new DenseMatrix(model.pc.numRows, model.pc.numCols,
        model.pc.values, model.pc.isTransposed), params.pcPath)
      Utils.writeVector(new DenseVector(model.explainedVariance.toArray), params.sigmaPath)
    }

    costTime
  }
}
