// scalastyle:off

package com.bigdata.graph
import com.bigdata.utils.Utils

import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter, InputStreamReader}
import java.util
import scala.beans.BeanProperty

class WLPAConfig extends Serializable {
  @BeanProperty var wlpa: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class WLPAParams extends Serializable {
  @BeanProperty var splitGraph: String = _
  @BeanProperty var commputePartition: Int = _
  @BeanProperty var maxIter: Int = _

  @BeanProperty var outputPath: String = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}


object WeightedLablePropagationRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName, isRaw) = (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val outputPath = args(2)
      val representer = new Representer
      representer.addClassTag(classOf[WLPAParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val stream: InputStreamReader = Utils.getStream("conf/graph/wlpa/wlpa.yml")
      val yaml = new Yaml(new Constructor(classOf[WLPAConfig]), representer, options)
      val description = new TypeDescription(classOf[WLPAParams])
      yaml.addTypeDescription(description)
      val config: WLPAConfig = yaml.load(stream).asInstanceOf[WLPAConfig]

      val params = new WLPAParams()
      val paramsMap =
        config.wlpa.get(isRaw match {
          case "no" => "opt"
          case _ => "raw"
        }).get(datasetName).asInstanceOf[util.HashMap[String, Object]]
      params.setSplitGraph(paramsMap.get("splitGraph").toString)
      params.setPartitions(paramsMap.get("commputePartition").toString.toInt)
      params.setMaxIter(paramsMap.get("maxIter").toString.toInt)
      params.setDatasetName(datasetName)
      params.setDataPath(dataPath)
      params.setOutputPath(outputPath)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("WLPA")
      params.setTestcaseType(s"${params.algorithmName}_${datasetName}_${isRaw}")

      val conf = new SparkConf().setAppName(params.testcaseType)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val sc = spark.sparkContext

      val startTime = System.currentTimeMillis()
      val inputRdd = Util.readDirectWeightDataFromHDFS(sc, params.dataPath, params.splitGraph, params.commputePartition)
        .map(f => Edge(f._1.toLong, f._2.toLong, f._3.toDouble))
      val graph = Graph.fromEdges(inputRdd, 1.0)
      val result = WLabelPropagation.run(graph, params.maxIter).vertices
      Util.saveDataToHDFS(result, ",", outputPath)
      val finishTime = System.currentTimeMillis()
      val costTime = (finishTime - startTime) / 1000

      params.setCostTime(costTime)
      println(s"Exec Successful: costTime: ${costTime}s")

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(
        s"report/${params.testcaseType}_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
