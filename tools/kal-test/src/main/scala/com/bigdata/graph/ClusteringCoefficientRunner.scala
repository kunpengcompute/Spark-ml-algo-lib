package com.bigdata.graph

import java.io.{File, FileWriter, InputStreamReader}
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ClusteringCoefficient
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty
class clusteringCoefficientConfig extends Serializable {
  @BeanProperty var clusteringCoefficient: util.HashMap[String, Object] = _
}
class clusteringCoefficientParms extends Serializable {
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
object ClusteringCoefficientRunner {

  def main(args: Array[String]): Unit = {
    try{
      val datsetName = args(0)
      val computePartitions = args(1).toInt
      val isWeight = args(2)
      val isRaw = args(3)
      val inputPath = args(4)
      val api = args(5)

      val weightedBool = isWeight match {
        case "weighted"   => true
        case "unweighted" => false
        case _            => throw new Exception("illegal weighted value")
      }

      val stream: InputStreamReader = Utils.getStream("conf/graph/clusteringcoefficient/clusteringcoefficient.yml")
      val representer = new Representer
      representer.addClassTag(classOf[clusteringCoefficientParms], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[clusteringCoefficientConfig]), representer, options)
      val description = new TypeDescription(classOf[clusteringCoefficientParms])
      yaml.addTypeDescription(description)
      val config: clusteringCoefficientConfig = yaml.load(stream).asInstanceOf[clusteringCoefficientConfig]
      val paramsMap: util.HashMap[String, Object] = config.clusteringCoefficient
        .get(datsetName)
        .asInstanceOf[util.HashMap[String, Object]]

      val params = new clusteringCoefficientParms

      val outputPath: String = paramsMap.get("outputPath").toString
      val splitGraph: String = paramsMap.get("splitGraph").toString
      val isDirect: Boolean = paramsMap.get("isDirect").toString.toBoolean

      params.setDatasetName(datsetName)
      params.setComputePartitions(computePartitions)
      params.setIsWeight(isWeight)
      params.setInputPath(inputPath)
      params.setIsRaw(isRaw)
      params.setApiName(api)
      params.setOutputPath(s"${outputPath}_${isWeight}")
      params.setSplitGraph(splitGraph)
      params.setIsDirect(isDirect)
      params.setAlgorithmName("ClusteringCoefficient")
      params.setTestcaseType(s"ClusteringCoefficient_${datsetName}_${api}_${isWeight}")

      println("inputPath:" + inputPath)
      println("outputPath:" + outputPath)

      val appName = s"ClusteringCoefficient_${api}_${isWeight}_${datsetName}"

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
          val result = ClusteringCoefficient
            .runLocalClusteringCoefficient(graph, isDirect, weightedBool).vertices
          Util.saveDataToHDFS(result, ",", params.outputPath)
        case "avgcc" => ClusteringCoefficient
            .runAverageClusteringCoefficient(graph, isDirect, weightedBool)
        case "globalcc" => ClusteringCoefficient.runGlobalClusteringCoefficient(graph)
        case _ => throw new Exception("illegal api")
      }

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)
      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(
        s"report/ClusteringCoefficient_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      if (api.equals("lcc")){
        println(s"Exec Successful: costTime: ${costTime}")
      } else if (api.equals("avgcc")){
        params.setAverageClusteringCoefficient(result)
        println("Exec Successful: costTime: \t%.5f\nAvgerageClusteringCoefficient\t%.5f".format(costTime, result))
      } else {
        params.setLocalClusteringCoefficient(result)
        println("Exec Successful: costTime: \t%.5f\nGlobalClusteringCoefficient\t%.5f".format(costTime, result))
      }
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
