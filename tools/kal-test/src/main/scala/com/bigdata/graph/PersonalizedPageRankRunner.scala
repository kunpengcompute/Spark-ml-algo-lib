package com.bigdata.graph

import java.io.FileWriter
import java.util

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
class PPrConfig extends Serializable {
  @BeanProperty var ppr: util.HashMap[String, Object] = _
}

class PPrParams extends Serializable {
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
  @BeanProperty var sourcesPath: String = _
  @BeanProperty var source: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}
object PersonalizedPageRankRunner {

  def main(args: Array[String]): Unit = {

    try {
      val datasetName = args(0)
      val api = args(1)
      val numPartitions = args(2).toInt
      val inputPath = args(3)
      val isRaw = args(4)
      val source = args(5)
      var outputPath = args(6)

      val stream = Utils.getStream("conf/graph/ppr/ppr.yml")

      val representer = new Representer
      representer.addClassTag(classOf[PPrParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[PPrConfig]), representer, options)
      val description = new TypeDescription(classOf[PPrParams])
      yaml.addTypeDescription(description)

      val config: PPrConfig = yaml.load(stream).asInstanceOf[PPrConfig]

      val paramsMap = config.ppr.get(datasetName).asInstanceOf[util.HashMap[String, Object]]

      val params = new PPrParams()

      var appName = s"PPR_${api}_${datasetName}_${source}"
      if (isRaw.equals("yes")) {
        appName = s"PPR_${api}_${datasetName}_${source}_raw"
      }

      val splitGraph = paramsMap.get("splitGraph").toString
      val numIter = paramsMap.get("numIter").toString.toInt
      val resetProb = paramsMap.get("resetProb").toString.toDouble
      val tolerance = paramsMap.get("tolerance").toString.toDouble
      val sourcesPath = s"${paramsMap.get("sourcesPath").toString}/${source}.txt"

      params.setApiName(api)
      params.setDatasetName(datasetName)
      params.setInputPath(inputPath)
      params.setIsRaw(isRaw)
      params.setNumIter(numIter)
      params.setNumPartitions(numPartitions)
      params.setOutputPath(outputPath)
      params.setResetProb(resetProb)
      params.setSourcesPath(sourcesPath)
      params.setTolerance(tolerance)
      params.setSource(source)
      params.setAlgorithmName("PPr")
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName)
      val sc = new SparkContext(conf)

      val startTime = System.currentTimeMillis()

      val inputRDD = Util.readUndirectDataFromHDFS(sc, inputPath, splitGraph, numPartitions)
        .map(f => (f._1.toLong, f._2.toLong))
      val graph = Util.convertToGraphXGraph(inputRDD)

      val result = api match {
        case "fixMS" =>
          val sourcesId = sc.textFile(sourcesPath).map(_.toLong).collect()
          PageRank.runParallelPersonalizedPageRank(graph, numIter, resetProb, sourcesId)
        case "fixSS" =>
          PageRank.runWithOptions(graph, numIter, resetProb, Option(source.toLong))
        case "conSS" =>
          PageRank.runUntilConvergenceWithOptions(graph, tolerance, resetProb, Option(source.toLong))
        case _ =>
          val empty = sc.emptyRDD[Edge[Double]]
          Graph.fromEdges(empty, 0.0)
      }

      Util.saveDataToHDFS(result.vertices.map(f => s"${f._1}\t${f._2}"), outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)


      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/PPR_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
