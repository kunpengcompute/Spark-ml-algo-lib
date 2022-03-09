package com.bigdata.ml

import com.bigdata.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org. apache.hadoop.fs.{FileSystem, Path}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter}
import java.util
import scala.beans.BeanProperty

class CovConfig extends Serializable {

  @BeanProperty var cov: util.HashMap[String, Object] = _
}

class CovParams extends Serializable {
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var datasetCpuName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var costTime: Double = _
  @BeanProperty var resultSum: Double = _
  @BeanProperty var evaluation: String = "mismatch"
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _


}

class CovKernel {
  def runJob(spark: SparkSession,params: CovParams): (Double, Double) = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val startTime = System.currentTimeMillis()

    val data = sc.textFile(params.dataPath)
      .map(x => Vectors.dense(x.split(",").map(_.toDouble)))
      .repartition(params.numPartitions)
      .persist(StorageLevel.MEMORY_ONLY)

    val matrix = new RowMatrix(data)
    val covMat = matrix.computeCovariance()
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0

    val result = covMat.toArray

    val saveFile = new Path(params.saveDataPath)
    if (fs.exists(saveFile)) {
      fs.delete(saveFile, true)
    }
    val res = new Array[String](covMat.numRows)
    for(i <- 0 until covMat.numRows) {
      val row = result.slice(i * covMat.numRows, (i + 1) * covMat.numCols).mkString(";")
      res(i) = i.toString + " " + row
    }
    sc.parallelize(res).repartition(100).saveAsTextFile(params.saveDataPath)

    if (params.isRaw == "yes") {
      val verifiedFile = new Path(params.verifiedDataPath)
      if (fs.exists(verifiedFile)) {
        val ori = sc.textFile(params.saveDataPath).map(line => {
          val arr = line.split(" ")
          (arr(0).toInt, arr(1).split(";").map(_.toDouble))}).collect().sortBy(_._1).map(_._2)
        val opt = sc.textFile(params.verifiedDataPath).map(line => {
          val arr = line.split(" ")
          (arr(0).toInt, arr(1).split(";").map(_.toDouble))}).collect().sortBy(_._1).map(_._2)

        Array.range(0, ori.length).foreach(i => {
          val arr1:Array[Double] = ori(i)
          val arr2:Array[Double] = opt(i)
          for (j <- 0 until arr1.length) {
            if(math.abs(arr1(j) - arr2(j)) <= 1.0e-7) {
              params.setEvaluation("match")
            }
          }
        })
      }
      else {
        params.setEvaluation("no verified data")
      }
    }
    (result.sum, costTime)
  }
}

object CovRunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName) = (modelConfSplit(0), modelConfSplit(1))

      val dataPath = args(1)

      val datasetCpuName = s"${datasetName}_${platformName}"
      val stream = Utils.getStream("conf/ml/cov/cov.yml")

      val representer = new Representer
      representer.addClassTag(classOf[CovParams], Tag.MAP)

      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)

      val yaml = new Yaml(new Constructor(classOf[CovConfig]), representer, options)
      val description = new TypeDescription(classOf[CovParams])
      yaml.addTypeDescription(description)
      val config: CovConfig = yaml.load(stream).asInstanceOf[CovConfig]
      val paramsMap = config.cov.get(datasetCpuName).asInstanceOf[util.HashMap[String, Object]]

      val params = new CovParams()

      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setDatasetCpuName(datasetCpuName)
      params.setAlgorithmName("Cov")
      params.setTestcaseType(s"Cov_${datasetName}_${platformName}")


      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)
      params.setSaveDataPath(paramsMap.get("saveDataPath").toString)
      params.setVerifiedDataPath(paramsMap.get("verifiedDataPath").toString)

      val conf = new SparkConf().setAppName(s"Cov_${datasetName}_${platformName}")

      if (platformName == "raw") {
        params.setIsRaw("yes")
      }
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val cov = new CovKernel()
      val tuple = cov.runJob(spark, params)
      params.setResultSum(tuple._1)
      params.setCostTime(tuple._2)
      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/Cov${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${params.getCostTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
