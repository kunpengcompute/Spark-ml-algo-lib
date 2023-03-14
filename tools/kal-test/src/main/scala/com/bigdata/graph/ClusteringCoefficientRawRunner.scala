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
import org.apache.spark.graphx.lib.LocalClusteringCoefficient
import org.apache.spark.{SparkConf, SparkContext}
class ClusteringCoefficientRawConfig extends Serializable {
  @BeanProperty var clusteringCoefficient: util.HashMap[String, util.HashMap[String, Object]] = _
}
class ClusteringCoefficientRawParms extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var isDirect: Boolean = _
  @BeanProperty var isWeight: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var computePartitions: Int = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var LocalClusteringCoefficient: AnyVal = _
  @BeanProperty var AverageClusteringCoefficient: AnyVal = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}
object ClusteringCoefficientRawRunner {

  def main(args: Array[String]): Unit = {
    try {
      val datsetName = args(0)
      val computePartitions = args(1).toInt
      val isWeight = args(2)
      val isRaw = args(3)
      val inputPath = args(4)
      val api = args(5)
      val outputPath = args(6)

      val weightedBool = isWeight match {
        case "weighted" => true
        case "unweighted" => false
        case _ => throw new Exception("illegal weighted value")
      }

      val stream: InputStreamReader = Utils.getStream("conf/graph/clusteringcoefficient/clusteringcoefficient.yml")
      val representer = new Representer
      representer.addClassTag(classOf[ClusteringCoefficientRawParms], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[ClusteringCoefficientRawConfig]), representer, options)
      val description = new TypeDescription(classOf[ClusteringCoefficientRawParms])
      yaml.addTypeDescription(description)
      val config: ClusteringCoefficientRawConfig = yaml.load(stream).asInstanceOf[ClusteringCoefficientRawConfig]
      val paramsMap: util.HashMap[String, Object] = config.clusteringCoefficient
        .get(isRaw match {
          case "no" => "opt"
          case _ => "raw"
        })
        .get(datsetName)
        .asInstanceOf[util.HashMap[String, Object]]

      val params = new ClusteringCoefficientRawParms

      val splitGraph: String = paramsMap.get("splitGraph").toString
      val isDirect: Boolean = paramsMap.get("isDirect").toString.toBoolean

      params.setDatasetName(datsetName)
      params.setComputePartitions(computePartitions)
      params.setIsWeight(isWeight)
      params.setInputPath(inputPath)
      params.setIsRaw(isRaw)
      params.setApiName(api)
      params.setOutputPath(outputPath)
      params.setSplitGraph(splitGraph)
      params.setIsDirect(isDirect)
      params.setAlgorithmName("ClusteringCoefficient")
      params.setTestcaseType(s"ClusteringCoefficient_${datsetName}_${api}_${isWeight}_Raw")

      println("inputPath:" + inputPath)
      println("outputPath:" + outputPath)

      val appName = s"${params.testcaseType}"

      val sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      val startTime: Long = System.currentTimeMillis()

      val inputRDD = Util.readUndirectDataFromHDFS(sc, inputPath, splitGraph, params.getComputePartitions)
        .map(f => (f._1.toLong, f._2.toLong))

      val graph = if (weightedBool) {
        Graph.fromEdgeTuples(inputRDD, 0.0).mapEdges(f => 1.0)
      } else {
        Graph.fromEdgeTuples(inputRDD, 0.0)
      }

      val result = api match {
        case "lcc" =>
          val result = LocalClusteringCoefficient.run(graph).vertices
          Util.saveDataToHDFS(result, ",", params.outputPath)
        case "avgcc" =>
          val result = LocalClusteringCoefficient.run(graph)
          var res = 0.0
          val gNum = graph.vertices.count()
          if(gNum != 0) {
            res = result.vertices.map(_._2).sum() / gNum
          }
          params.setAverageClusteringCoefficient(res)
          println(res)
        case "globalcc" =>
          val result: Double = LocalClusteringCoefficient.runGlobalClusteringCoefficient(graph)
          params.setLocalClusteringCoefficient(result)
          println(result)
        case _ => throw new Exception("illegal api")
      }

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Exec Successful: costTime: ${costTime}s")
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/${params.testcaseType}_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      sc.stop()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
