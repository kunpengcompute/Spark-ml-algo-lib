package com.bigdata.graph

import java.io.{File, FileWriter}
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.graphx.lib.WeakCliqueEnumeration
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class WceConfig extends Serializable {
  @BeanProperty var wce: util.HashMap[String, Object] = _
}

class WceParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var maxIterations: Int = _
  @BeanProperty var maxDegree: Int = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object WCERunner {

  def main(args: Array[String]): Unit = {
    try {
      val sparkConf = new SparkConf().setAppName("WCE").setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      val datasetName = args(0)

      // record start time
      val startTime = System.currentTimeMillis()

      val stream = Utils.getStream("conf/graph/wce/wce.yml")

      val representer = new Representer
      representer.addClassTag(classOf[WceParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[WceConfig]), representer, options)
      val description = new TypeDescription(classOf[WceParams])
      yaml.addTypeDescription(description)
      val config: WceConfig = yaml.load(stream).asInstanceOf[WceConfig]
      val paramsMap = config.wce.get(datasetName).asInstanceOf[util.HashMap[String, Object]]

      val params = new WceParams()

      val inputPath = paramsMap.get("inputPath").toString
      val outputPath = paramsMap.get("outputPath").toString
      val splitGraph = paramsMap.get("splitGraph").toString
      val maxIterations = paramsMap.get("maxIterations").toString.toInt
      val maxDegree = paramsMap.get("maxDegree").toString.toInt
      val numPartitions = paramsMap.get("numPartitions").toString.toInt

      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setSplitGraph(splitGraph)
      params.setMaxIterations(maxIterations)
      params.setMaxDegree(maxDegree)
      params.setNumPartitions(numPartitions)
      params.setDatasetName(datasetName)
      params.setAlgorithmName("Wce")
      params.setTestcaseType(s"Wce_${datasetName}")

      println("inputPath: " + inputPath)
      println("outputPath: " + outputPath)

      val inputRdd = Util.readUndirectDataFromHDFS(sc, inputPath, splitGraph, numPartitions)
      val result = WeakCliqueEnumeration.run(inputRdd, maxIterations.toInt, maxDegree.toInt)
      Util.saveDataToHDFS(result._1, ",", outputPath + "/map_info")
      Util.saveDataToHDFS(result._2, ",", outputPath + "/clique_info")

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println("weak clique detection result = true, and costTime = " + costTime + "s")

      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/WCE_${
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
