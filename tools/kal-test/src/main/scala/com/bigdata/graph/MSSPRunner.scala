package com.bigdata.graph

import java.io.FileWriter
import java.util

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

class MsspConfig extends Serializable {
  @BeanProperty var mssp: util.HashMap[String, Object] = _
}

class MsspParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var sourcePath: String = _
  @BeanProperty var computePartition: Int = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object MSSPRunner {
  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val computePartition = args(1).toInt
      val inputPath = args(2)
      val outputPath = args(3)
      val sourcePath = args(4)
      val splitGraph = args(5)
      val isRaw = args(6)

      val params = new MsspParams()
      params.setInputPath(inputPath)
      params.setSourcePath(sourcePath)
      params.setComputePartition(computePartition)
      params.setSplitGraph(splitGraph)
      params.setOutputPath(outputPath)
      params.setDatasetName(datasetName)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("Mssp")

      println("inputPath: " + inputPath)
      println("sourcePath: " + sourcePath)
      println("computePartition: " + computePartition)
      println("splitGraph: " + splitGraph)
      println("outputPath: " + outputPath)
      println("datasetName: " + datasetName)
      println("isRaw: " + isRaw)
      var appName = s"MSSP_${datasetName}"
      if (isRaw.equals("yes")) {
        appName = s"MSSP_RAW_${datasetName}"
      }

      params.setTestcaseType(appName)

      val sparkConf = new SparkConf().setAppName(appName).setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      // record start time
      val startTime = System.currentTimeMillis()

      val edgeRDD = Util.readUndirectDataFromHDFS(sc, inputPath, splitGraph, computePartition)
        .map(f => (f._1.trim.toLong, f._2.trim.toLong)).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val inputGraph = Util.convertToGraphXGraph(edgeRDD).persist()
      val source = Util.loadSourceVertices(sc, sourcePath)

      val res = ShortestPaths.run(inputGraph, source.collect.toSeq).vertices
      res.map(f => {
        var s = f._1.toString + ":"
          f._2.foreach(x => s = s + "(" + x._1.toString + "," + x._2.toString + ")")
        s
      }).saveAsTextFile(outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println("Short Path result = true, and costTime = " + costTime + "s")

      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(s"report/MSSP_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      val representer = new Representer
      representer.addClassTag(classOf[MsspParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[MsspConfig]), representer, options)
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
