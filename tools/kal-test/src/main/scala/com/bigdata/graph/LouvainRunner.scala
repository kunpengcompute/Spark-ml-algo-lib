package com.bigdata.graph

import java.io.FileWriter
import java.util

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.graphx.lib.Louvain
import org.apache.spark.{SparkConf, SparkContext}

class LouvainConfig extends Serializable {
  @BeanProperty var louvain: util.HashMap[String, Object] = _
}

class LouvainParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var maxIterations: Int = _
  @BeanProperty var isDirected: Boolean = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var modularity: Double = _
  @BeanProperty var communityNum: Int = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _

}

object LouvainRunner {

  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val numPartitions = args(1).toInt
      val isRaw = args(2)
      val inputPath = args(3)
      val outputPath = args(4)

      val stream = Utils.getStream("conf/graph/louvain/louvain.yml")

      val representer = new Representer
      representer.addClassTag(classOf[LouvainParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[LouvainConfig]), representer, options)
      val description = new TypeDescription(classOf[LouvainParams])
      yaml.addTypeDescription(description)
      val config: LouvainConfig = yaml.load(stream).asInstanceOf[LouvainConfig]
      val paramsMap =
        config.louvain
          .get(datasetName)
          .asInstanceOf[util.HashMap[String, Object]]

      val params = new LouvainParams()

      val splitGraph = paramsMap.get("splitGraph").toString
      val maxIterations = paramsMap.get("maxIterations").toString.toInt
      val isDirected = paramsMap.get("isDirected").toString.toBoolean

      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setSplitGraph(splitGraph)
      params.setMaxIterations(maxIterations)
      params.setIsDirected(isDirected)
      params.setDatasetName(datasetName)
      params.setNumPartitions(numPartitions)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("Louvain")
      params.setTestcaseType(s"Louvain_${datasetName}")

      println("inputPath: " + inputPath)
      println("outputPath: " + outputPath)

      var appName = s"Louvain_${datasetName}"
      val sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      // record start time
      val startTime = System.currentTimeMillis()

      val edgeRDD =
        Util.readGraphFromHDFS(sc, inputPath, splitGraph, false, numPartitions)
      val (q, comm) = Louvain.run(edgeRDD, maxIterations, isDirected, numPartitions)
      Util.saveDataToHDFS(comm, ",", outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      val commNum = comm.map(_._2).distinct().count().toInt
      println("louvain modularity:\t%.5f\nComm num:\t%d\nCost time:\t%.5f".format(q, commNum, costTime))
      params.setCostTime(costTime)
      params.setModularity(q)
      params.setCommunityNum(commNum)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/Louvain_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
