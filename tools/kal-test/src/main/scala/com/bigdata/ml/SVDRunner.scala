package com.bigdata.ml

import java.io.{File, FileWriter, PrintWriter}
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.storage.StorageLevel
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, norm => brzNorm}

import scala.beans.BeanProperty

class SVDConfig extends Serializable {

  @BeanProperty var svd: util.HashMap[String, util.HashMap[String, Object]] = _
}

class SVDParams extends Serializable {

  @BeanProperty var pt: Int = _
  @BeanProperty var k: Int = _
  @BeanProperty var sep: String = _
  @BeanProperty var dataFormat: String = _
  @BeanProperty var numCols: Int = _
  @BeanProperty var numRows: Int = _

  @BeanProperty var inputDataPath: String = _
  @BeanProperty var outputDataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object SVDRunner {
  def main(args: Array[String]): Unit = {

    try {
      val datasetName = args(0)

      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (inputDataPath, outputDataPath) = (dataPathSplit(0), dataPathSplit(1))

      val cpuName = args(2)
      val isRaw = args(3)
      val sparkConfSplit = args(4).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))

      val stream = isRaw match {
        case "no" =>
          Utils.getStream("conf/ml/svd/svd.yml")
        case "yes" =>
          Utils.getStream("conf/ml/svd/svd_raw.yml")
      }
      val representer = new Representer
      representer.addClassTag(classOf[SVDParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SVDConfig]), representer, options)
      val description = new TypeDescription(classOf[SVDParams])
      yaml.addTypeDescription(description)
      val configs: SVDConfig = yaml.load(stream).asInstanceOf[SVDConfig]
      val params = new SVDParams()

      val svdParamMap: util.HashMap[String, Object] = configs.svd.get(datasetName)
      params.setPt(svdParamMap.getOrDefault("pt", "250").asInstanceOf[Int])
      params.setK(svdParamMap.getOrDefault("k", "10").asInstanceOf[Int])
      params.setSep(svdParamMap.getOrDefault("sep", ",").asInstanceOf[String])
      params.setDataFormat(svdParamMap.getOrDefault("dataFormat", "dense").asInstanceOf[String])
      params.setNumCols(svdParamMap.getOrDefault("numCols", "0").asInstanceOf[Int])
      params.setNumRows(svdParamMap.getOrDefault("numRows", "0").asInstanceOf[Int])

      params.setInputDataPath(inputDataPath)
      params.setOutputDataPath(outputDataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("SVD")

      var appName = s"SVD_${datasetName}"
      if (isRaw == "yes") {
        appName = s"SVD_${datasetName}_raw"
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

      val costTime = new SVDKernel().runJob(spark, params)
      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/SVD_${
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

class SVDKernel {

  def runJob(spark: SparkSession, params: SVDParams): Double = {

    import spark.implicits._
    val startTime = System.currentTimeMillis()
    val numColsBC = spark.sparkContext.broadcast(params.numCols)
    val sepBC = spark.sparkContext.broadcast(params.sep)
    val trainingData = if (params.dataFormat == "coo") {
      spark.sparkContext.textFile(params.inputDataPath, params.pt)
        .map(line => {
          val entry = line.split(sepBC.value)
          (entry(0).toInt, (entry(1).toInt, entry(2).toDouble))
        }).groupByKey()
        .map{case (_, vectorEntries) => {Vectors.sparse(numColsBC.value, vectorEntries.toSeq)}}
        .persist(StorageLevel.MEMORY_ONLY)
    } else {
      spark.sparkContext.textFile(params.inputDataPath)
        .map(row => Vectors.dense(row.split(sepBC.value).map(_.toDouble)))
        .repartition(params.pt)
        .persist(StorageLevel.MEMORY_ONLY)
    }

    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)

    val matrix = new RowMatrix(trainingData, params.numRows, params.numCols)

    val svd = matrix.computeSVD(params.k, computeU  = true)

    //触发U的计算
    svd.U.rows.count()

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(costTime)

    // save V and s
    if (params.outputDataPath != null){
      spark.sparkContext.parallelize(svd.s.toArray, 1).saveAsTextFile(s"${params.outputDataPath}_${params.cpuName}_${params.isRaw}/s")
      spark.sparkContext.parallelize(Utils.toRowMajorArray(svd.V.asInstanceOf[DenseMatrix]).map(_.mkString(","))).saveAsTextFile(s"${params.outputDataPath}_${params.cpuName}_${params.isRaw}/V")
    }

    println(s"Results have been saved at ${params.outputDataPath}")

    costTime
  }

}
