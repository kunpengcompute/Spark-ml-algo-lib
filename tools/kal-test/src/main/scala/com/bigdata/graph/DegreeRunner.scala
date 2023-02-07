package com.bigdata.graph

import java.io.FileWriter
import java.util

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}

class DegreeConfig extends Serializable {
  @BeanProperty var degree: util.HashMap[String, Object] = _
}

class DegreeParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var apiName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _

}

object DegreeRunner {
  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val api = args(1)
      val numPartitions = args(2).toInt
      val isRaw = args(3)
      val inputPath = args(4)
      val outputPath = args(5)

      val stream = Utils.getStream("conf/graph/degree/degree.yml")
      val representer = new Representer
      representer.addClassTag(classOf[DegreeParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[DegreeConfig]), representer, options)
      val description = new TypeDescription(classOf[DegreeParams])
      yaml.addTypeDescription(description)
      val config: DegreeConfig = yaml.load(stream).asInstanceOf[DegreeConfig]
      val paramsMap =
        config.degree
          .get(datasetName)
          .asInstanceOf[util.HashMap[String, Object]]
      val splitGraph = paramsMap.get("splitGraph").toString

      val params = new DegreeParams()
      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setSplitGraph(splitGraph)
      params.setDatasetName(datasetName)
      params.setNumPartitions(numPartitions)
      params.setApiName(api)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("Degree")

      println("inputPath: " + inputPath)
      println("outputPath: " + outputPath)

      var appName = s"Degree_${datasetName}_${api}"
      if (isRaw == "yes") {
        appName = s"Degree_${datasetName}_${api}_raw"
      }
      val sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
      val sc = new SparkContext(sparkConf)
      params.setTestcaseType(appName)

      // record start time
      val startTime = System.currentTimeMillis()
      val edgeInfo =
        Util.readDataFromHDFSForDegree(sc, inputPath, splitGraph, numPartitions)
      val graph = Graph.fromEdgeTuples(edgeInfo, 0)
      val result = api match {
        case "degrees" => graph.degrees
        case "inDegrees" => graph.inDegrees
        case "outDegrees" => graph.outDegrees
        case _ => throw new Exception("illegal api")
      }

      Util.saveDataToHDFS(result, ",", outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/Degree_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
