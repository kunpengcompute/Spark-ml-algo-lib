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
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.{SparkConf, SparkContext}

class PrConfig extends Serializable {
  @BeanProperty var pr: util.HashMap[String, Object] = _
}

class PrParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var numIter: Int = _
  @BeanProperty var resetProb: Double = _
  @BeanProperty var tolerance: Double = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var apiName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object PageRankRunner {

  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val api = args(1)
      val numPartitions = args(2).toInt
      val isRaw = args(3)
      val inputPath = args(4)
      val outputPath = args(5)

      val stream = Utils.getStream("conf/graph/pr/pr.yml")

      val representer = new Representer
      representer.addClassTag(classOf[PrParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[PrConfig]), representer, options)
      val description = new TypeDescription(classOf[PrParams])
      yaml.addTypeDescription(description)
      val config: PrConfig = yaml.load(stream).asInstanceOf[PrConfig]
      val paramsMap =
        config.pr.get(datasetName).asInstanceOf[util.HashMap[String, Object]]

      val params = new PrParams()

      val splitGraph = paramsMap.get("splitGraph").toString
      val numIter = paramsMap.get("numIter").toString.toInt
      val resetProb = paramsMap.get("resetProb").toString.toDouble
      var tolerance = paramsMap.get("tolerance").toString.toDouble

      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setSplitGraph(splitGraph)
      params.setNumIter(numIter)
      params.setResetProb(resetProb)
      params.setTolerance(tolerance)
      params.setDatasetName(datasetName)
      params.setNumPartitions(numPartitions)
      params.setApiName(api)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("Pr")

      println("inputPath: " + inputPath)
      println("outputPath: " + outputPath)

      var appName = s"PageRank_${datasetName}_${api}"
      if (isRaw == "yes") {
        appName = s"PageRank_${datasetName}_${api}_raw"
      }
      params.setTestcaseType(appName)

      val sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      // record start time
      val startTime = System.currentTimeMillis()

      implicit val context = sc
      val edges =
        Util
          .loadLines2EdgesT(inputPath, numPartitions, 1, false, splitGraph)
          .setName("OriginalEdges")
      val graph = Graph.fromEdges(edges, 0.15D)
      val result = api match {
        case "runUntilConvergence" =>
          PageRank.runUntilConvergence(graph, tolerance, resetProb)
        case "run" => PageRank.run(graph, numIter, resetProb)
        case _ => throw new Exception("illegal api")
      }

      result.vertices.map(f => s"${f._1}\t${f._2}").saveAsTextFile(outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println("pagerank costTime = " + costTime + "s")

      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/PR_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
