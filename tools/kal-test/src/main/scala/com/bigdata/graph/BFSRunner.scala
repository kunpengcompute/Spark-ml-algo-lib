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
import org.apache.spark.graphx.lib.BFS
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class BFSConfig extends Serializable {
  @BeanProperty var bfs: util.HashMap[String, Object] = _
}

class BFSParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isDirect: Boolean = _
  @BeanProperty var sourceID: Long = _
  @BeanProperty var depthLimit: Int = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object BFSRunner {
  def main(args: Array[String]): Unit = {
    try {
      val datasetName: String = args(0)
      val sourceID: Long = args(1).toLong
      val numPartitions: Int = args(2).toInt
      val isRaw: String = args(3)
      val inputPath: String = args(4)
      val outputPath: String = args(5)

      val stream: InputStreamReader = Utils.getStream("conf/graph/bfs/bfs.yml")
      val representer = new Representer
      representer.addClassTag(classOf[BFSParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[BFSConfig]), representer, options)
      val description = new TypeDescription(classOf[BFSParams])
      yaml.addTypeDescription(description)
      val config: BFSConfig = yaml.load(stream).asInstanceOf[BFSConfig]
      val paramsMap: util.HashMap[String, Object] =
        config.bfs
          .get(datasetName)
          .asInstanceOf[util.HashMap[String, Object]]

      val params = new BFSParams()

      val splitGraph: String = paramsMap.get("splitGraph").toString
      val isDirect: Boolean = paramsMap.get("isDirect").toString.toBoolean
      val depthLimit: Int = paramsMap.get("depthLimit").toString.toInt

      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setSplitGraph(splitGraph)
      params.setDatasetName(datasetName)
      params.setNumPartitions(numPartitions)
      params.setIsDirect(isDirect)
      params.setSourceID(sourceID)
      params.setDepthLimit(depthLimit)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("BFS")
      params.setTestcaseType(s"BFS_${datasetName}_${sourceID}")
      println("inputPath: " + inputPath)
      println("outputPath: " + outputPath)

      val appName = s"BFS_${datasetName}_${sourceID}"
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      // record start time
      val startTime: Long = System.currentTimeMillis()
      val inputRdd: RDD[(String, String)] =
        Util.readUndirectDataFromHDFS(sc, inputPath, splitGraph, numPartitions)
      val graph: Graph[Int, Int] = Util.buildUnweightedGraph(inputRdd, isDirect)

      val result: RDD[String] =
        BFS
          .run(graph, sourceID, isDirect, depthLimit)
          .vertices
          .filter(_._2._1 != Integer.MAX_VALUE)
          .map(f => f._1.toString + ";" + f._2._2.mkString(",") + ";" + f._2._1)

      Util.saveDataToHDFS(result, outputPath)

      val costTime: Double = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/BFS_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }

}
