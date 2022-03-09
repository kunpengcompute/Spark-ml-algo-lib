package com.bigdata.graph

import java.io.{File, FileWriter}
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.graphx.lib.Modularity
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class ModularityConfig extends Serializable {
  @BeanProperty var modularity: util.HashMap[String, Object] = _
}

class ModularityParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var inputCommunity: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var splitCommunity: String = _
  @BeanProperty var isWeighted: Boolean = _
  @BeanProperty var isDirected: Boolean = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object ModularityRunner {

  def main(args: Array[String]): Unit = {
    try {
      val sparkConf = new SparkConf().setAppName("Modularity").setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      val datasetName = args(0)

      val stream = Utils.getStream("conf/graph/modularity/modularity.yml")

      val representer = new Representer
      representer.addClassTag(classOf[ModularityParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[ModularityConfig]), representer, options)
      val description = new TypeDescription(classOf[ModularityParams])
      yaml.addTypeDescription(description)
      val config: ModularityConfig = yaml.load(stream).asInstanceOf[ModularityConfig]
      val paramsMap = config.modularity.get(datasetName).asInstanceOf[util.HashMap[String, Object]]

      val params = new ModularityParams()

      val inputPath = paramsMap.get("inputPath").toString
      val inputCommunity = paramsMap.get("inputCommunity").toString
      val splitGraph = paramsMap.get("splitGraph").toString
      val splitCommunity = paramsMap.get("splitCommunity").toString
      val isWeighted = paramsMap.get("isWeighted").toString.toBoolean
      val isDirected = paramsMap.get("isDirected").toString.toBoolean
      val numPartitions = paramsMap.get("numPartitions").toString.toInt

      params.setInputPath(inputPath)
      params.setInputCommunity(inputCommunity)
      params.setSplitGraph(splitGraph)
      params.setSplitCommunity(splitCommunity)
      params.setIsDirected(isDirected)
      params.setIsWeighted(isWeighted)
      params.setNumPartitions(numPartitions)
      params.setDatasetName(datasetName)
      params.setAlgorithmName("Modularity")
      params.setTestcaseType(s"Modularity_${datasetName}")

      println("inputPath: " + inputPath)
      println("inputCommunity: " + inputCommunity)

      // record start time
      val startTime = System.currentTimeMillis()
      val graphRDD = Util.readGraphFromHDFS(sc, inputPath, splitGraph, isWeighted, numPartitions)
      val communityRDD = Util.readCommFromHDFS(sc, inputCommunity, splitCommunity, numPartitions)
      val q = Modularity.run(communityRDD, graphRDD, isDirected, numPartitions)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println("modularity: %.5f\nCost time: %.5f".format(q, costTime))

      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/Modularity_${
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
