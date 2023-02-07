package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.SVDVerify

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import breeze.linalg.{scale, DenseMatrix => BDM, DenseVector => BDV, norm => brzNorm}

import java.io.{File, FileWriter, PrintWriter}
import java.util
import scala.beans.BeanProperty

class SVDConfig extends Serializable {

  @BeanProperty var svd: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class SVDParams extends Serializable {

  @BeanProperty var pt: Int = _
  @BeanProperty var k: Int = _
  @BeanProperty var sep: String = _
  @BeanProperty var dataFormat: String = _
  @BeanProperty var numCols: Int = _
  @BeanProperty var numRows: Int = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object SVDRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val sparkConfSplit = args(3).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))
      val saveResultPath = args(4)

      val stream = Utils.getStream("conf/ml/svd/svd.yml")
      val representer = new Representer
      representer.addClassTag(classOf[SVDParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SVDConfig]), representer, options)
      val description = new TypeDescription(classOf[SVDParams])
      yaml.addTypeDescription(description)
      val configs: SVDConfig = yaml.load(stream).asInstanceOf[SVDConfig]
      val paramsMap: util.HashMap[String, Object] = configs.svd.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      val params = new SVDParams()
      params.setPt(paramsMap.getOrDefault("pt", "250").asInstanceOf[Int])
      params.setK(paramsMap.getOrDefault("k", "10").asInstanceOf[Int])
      params.setSep(paramsMap.getOrDefault("sep", ",").asInstanceOf[String])
      params.setDataFormat(paramsMap.getOrDefault("dataFormat", "dense").asInstanceOf[String])
      params.setNumCols(paramsMap.getOrDefault("numCols", "0").asInstanceOf[Int])
      params.setNumRows(paramsMap.getOrDefault("numRows", "0").asInstanceOf[Int])
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("SVD")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${datasetName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${datasetName}_raw"
        params.setVerifiedDataPath(params.saveDataPath)
        params.setSaveDataPath(s"${params.saveDataPath}_raw")
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName).setMaster(master)
      val commonParas = Array(
        ("spark.submit.deployMode", deployMode),
        ("spark.executor.instances", numExec),
        ("spark.executor.cores", execCores),
        ("spark.executor.memory", execMem)
      )
      conf.setAll(commonParas)
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val costTime = new SVDKernel().runJob(spark, params)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(SVDVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
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
class SVDKernel {
  def runJob(spark: SparkSession, params: SVDParams): Double = {

    import spark.implicits._
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val startTime = System.currentTimeMillis()
    val numColsBC = sc.broadcast(params.numCols)
    val sepBC = sc.broadcast(params.sep)
    val trainingData = if (params.dataFormat == "coo") {
      sc.textFile(params.dataPath, params.pt)
        .map(line => {
          val entry = line.split(sepBC.value)
          (entry(0).toInt, (entry(1).toInt, entry(2).toDouble))
        }).groupByKey()
        .map{case (_, vectorEntries) => {Vectors.sparse(numColsBC.value, vectorEntries.toSeq)}}
        .persist(StorageLevel.MEMORY_ONLY)
    } else {
      sc.textFile(params.dataPath)
        .map(row => Vectors.dense(row.split(sepBC.value).map(_.toDouble)))
        .repartition(params.pt)
        .persist(StorageLevel.MEMORY_ONLY)
    }

    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)

    val matrix = new RowMatrix(trainingData, params.numRows, params.numCols)
    val model = matrix.computeSVD(params.k, computeU  = true)
    model.U.rows.count()
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val sigmaPath = s"${params.saveDataPath}/s"
    val VPath = s"${params.saveDataPath}/V"
    val s = model.s.asInstanceOf[DenseVector]
    val V = model.V.asInstanceOf[DenseMatrix]
    SVDVerify.saveVector(s, sigmaPath, sc)
    SVDVerify.saveMatrix(V, VPath, sc)

    costTime
  }
}
