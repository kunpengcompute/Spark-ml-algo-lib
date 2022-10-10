package com.bigdata.graph

import java.io.{FileWriter, InputStreamReader}
import java.util

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.TrustRank
import org.apache.spark.{SparkConf, SparkContext}


class TrustRankConfig extends Serializable{
  @BeanProperty var trustRank: util.HashMap[String, Object] = _
}

class TrustRankParams extends Serializable{
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var seedsPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var computePartitions: Int = _
  @BeanProperty var tol: Double = _
  @BeanProperty var numIter: Int = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var resetProb: Double = _
  @BeanProperty var seedsCnt: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}


object TrustRankRunner {

  def main(args: Array[String]): Unit = {
    try {
      val datsetName = args(0)
      val computePartitions = args(1).toInt
      val isRaw = args(2)
      val inputPath = args(3)
      val api = args(4)
      val seedsCnt = args(5)
      val outputPath = args(6)

      val stream: InputStreamReader = Utils.getStream("conf/graph/tr/tr.yml")

      val representer = new Representer
      representer.addClassTag(classOf[TrustRankParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[TrustRankConfig]), representer, options)
      val description = new TypeDescription(classOf[TrustRankParams])
      yaml.addTypeDescription(description)
      val config: TrustRankConfig = yaml.load(stream).asInstanceOf[TrustRankConfig]
      val paramsMap: util.HashMap[String, Object] = config.trustRank
        .get(s"${datsetName}_${seedsCnt}")
        .asInstanceOf[util.HashMap[String, Object]]

      val params = new TrustRankParams

      val seedsPath: String = paramsMap.get("seedsPath").toString
      val splitGraph: String = paramsMap.get("splitGraph").toString
      val tol: Double = paramsMap.get("tolerance").toString.toDouble
      val numIter = paramsMap.get("numIter").toString.toInt
      val resetProb = paramsMap.get("resetProb").toString.toDouble

      params.setSeedsCnt(seedsCnt)
      params.setSeedsPath(seedsPath)
      params.setSplitGraph(splitGraph)
      params.setOutputPath(outputPath)
      params.setDatasetName(datsetName)
      params.setComputePartitions(computePartitions)
      params.setIsRaw(isRaw)
      params.setInputPath(inputPath)
      params.setApiName(api)
      params.setTol(tol)
      params.setNumIter(numIter)
      params.setResetProb(resetProb)
      params.setAlgorithmName("TrustRank")
      params.setTestcaseType(s"TrustRank_${datsetName}_${api}_${seedsCnt}")

      val appName = s"TrustRank_${datsetName}_${api}_${seedsCnt}"

      val sparkConf = new SparkConf()
        .setMaster("yarn")
        .setAppName(appName)
      val sc = new SparkContext(sparkConf)
      implicit val context = sc

      val startTime = System.currentTimeMillis()

      val edges = Util.loadLines2EdgesT(inputPath, computePartitions, 1.0D, false, splitGraph)
        .setName("OriginalEdges")
      val g = Graph.fromEdges(edges, 1.0D)
      val seeds = sc.textFile(seedsPath, 10).map(_.split("\t")(0).trim.toLong)

      api match {
        case "run" => TrustRank.run(g, seeds, numIter, resetProb)
            .vertices.map(f => s"${f._1}\t${f._2}")
            .saveAsTextFile(params.outputPath)
        case "runUntilConvergence" => TrustRank.runUntilConvergence(g, seeds, tol, resetProb)
            .vertices.map(f => s"${f._1}\t${f._2}")
            .saveAsTextFile(params.outputPath)
        case _ => throw new Exception("illegal api")
      }

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/TrustRank_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
