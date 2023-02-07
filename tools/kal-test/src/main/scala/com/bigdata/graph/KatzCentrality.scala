// scalastyle:off

package com.bigdata.graph
import com.bigdata.utils.Utils

import org.apache.spark.graphx.lib.{KatzCentrality, Parameters}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter, InputStreamReader}
import java.util
import scala.beans.BeanProperty

class KatzCentralityConfig extends Serializable {
  @BeanProperty var katz: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class KatzCentralityParams extends Serializable {
  @BeanProperty var splitGraph: String = _
  @BeanProperty var partitions: Int = _
  @BeanProperty var isWeight: Boolean = _
  @BeanProperty var tol: Double = _
  @BeanProperty var maxIter: Int = _
  @BeanProperty var normalized: Boolean = _

  @BeanProperty var outputPath: String = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}


object KatzCentralityRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName, isRaw) = (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val outputPath = args(2)
      val representer = new Representer
      representer.addClassTag(classOf[KatzCentralityParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val stream: InputStreamReader = Utils.getStream("conf/graph/katz/katz.yml")
      val yaml = new Yaml(new Constructor(classOf[KatzCentralityConfig]), representer, options)
      val description = new TypeDescription(classOf[KatzCentralityParams])
      yaml.addTypeDescription(description)
      val config: KatzCentralityConfig = yaml.load(stream).asInstanceOf[KatzCentralityConfig]

      val params = new KatzCentralityParams()
      val paramsMap =
        config.katz.get(isRaw match {
          case "no" => "opt"
          case _ => "raw"
        }).get(datasetName).asInstanceOf[util.HashMap[String, Object]]
      params.setSplitGraph(paramsMap.get("splitGraph").toString)
      params.setPartitions(paramsMap.get("partitions").toString.toInt)
      params.setIsWeight(paramsMap.get("isWeight").toString.toBoolean)
      params.setTol(paramsMap.get("tol").toString.toDouble)
      params.setMaxIter(paramsMap.get("maxIter").toString.toInt)
      params.setNormalized(paramsMap.get("normalized").toString.toBoolean)
      params.setDatasetName(datasetName)
      params.setDataPath(dataPath)
      params.setOutputPath(outputPath)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("KatzCentrality")
      params.setTestcaseType(s"${params.algorithmName}_${datasetName}_${isRaw}")

      val conf = new SparkConf().setAppName(params.testcaseType)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val sc = spark.sparkContext

      val startTime = System.currentTimeMillis()
      val edgeRDD = Util.readGraphFromHDFS(sc, params.dataPath, params.splitGraph, params.isWeight, params.partitions)
        .map(x => (x._1, x._2, x._3))
      val tmp = edgeRDD.map(f => Edge(f._1, f._2, f._3))
      val g: Graph[Double, Double] = Graph.fromEdges(tmp, 1.0)
      val result = KatzCentrality.run(g, params.maxIter, params.tol, params.normalized)
      result.map(f => (f._1, f._2)).saveAsTextFile(params.outputPath)
      val finishTime = System.currentTimeMillis()
      val costTime = (finishTime - startTime) / 1000

      params.setCostTime(costTime)
      println(s"Exec Successful: costTime: ${costTime}")

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
