package com.bigdata.graph

import java.io.{File, FileWriter, InputStreamReader}
import java.util
import com.bigdata.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.lib.{Node2Vec, Params}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class Node2VecConfig extends Serializable{
  @BeanProperty var node2vec: util.HashMap[String, Object] = _
}

class Node2VecParams extends Serializable{
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var partitions: Int = _
  @BeanProperty var directed: Boolean = _
  @BeanProperty var weighted: Boolean = _
  @BeanProperty var walkLength: Int = _
  @BeanProperty var numWalks: Int = _
  @BeanProperty var p: Double = _
  @BeanProperty var q: Double = _
  @BeanProperty var iteration: Int = _
  @BeanProperty var dimension: Int = _
  @BeanProperty var windowSize: Int = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object Node2VecRunner {

  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName) = (modelConfSplit(0), modelConfSplit(1))
      val inputPath = args(1)

      val representer = new Representer
      representer.addClassTag(classOf[Node2VecParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val stream: InputStreamReader = Utils.getStream("conf/graph/node2vec/node2vec.yml")
      val yaml = new Yaml(new Constructor(classOf[Node2VecConfig]), representer, options)
      val description = new TypeDescription(classOf[Node2VecParams])
      yaml.addTypeDescription(description)
      val config: Node2VecConfig = yaml.load(stream).asInstanceOf[Node2VecConfig]
      val paramsMap: util.HashMap[String, Object] =
        config.node2vec
          .get(datasetName)
          .asInstanceOf[util.HashMap[String, Object]]

      val params = new Node2VecParams()

      params.setDatasetName(datasetName)
      params.setInputPath(inputPath)
      params.setOutputPath(paramsMap.get("outputPath").toString)
      params.setPartitions(paramsMap.get("partitions").toString.toInt)
      params.setDirected(paramsMap.get("directed").toString.toBoolean)
      params.setWeighted(paramsMap.get("weighted").toString.toBoolean)
      params.setWalkLength(paramsMap.get("walkLength").toString.toInt)
      params.setNumWalks(paramsMap.get("numWalks").toString.toInt)
      params.setP(paramsMap.get("p").toString.toDouble)
      params.setQ(paramsMap.get("q").toString.toDouble)
      params.setIteration(paramsMap.get("iteration").toString.toInt)
      params.setDimension(paramsMap.get("dimension").toString.toInt)
      params.setWindowSize(paramsMap.get("windowSize").toString.toInt)
      params.setSplitGraph(paramsMap.get("splitGraph").toString)
      params.setAlgorithmName("Node2Vec")
      params.setTestcaseType(s"Node2Vec_${datasetName}")

      val conf = new SparkConf().setAppName(s"Node2Vec_${datasetName}_${platformName}")
      val sc = new SparkContext(conf)

      val startTime = System.currentTimeMillis()

      val edgeRDD = Util.readEdgeListFromHDFS(sc, inputPath, params.getSplitGraph, params.getWeighted, params.getPartitions)

      val n2vParams = Params(params.getDirected, params.getWeighted, params.getP, params.getQ, params.getWalkLength, params.getNumWalks, params.getIteration, params.getDimension, params.getWindowSize)

      val node2vecModel = Node2Vec.run(edgeRDD,n2vParams)
      Util.saveNode2VecModel(node2vecModel, params.getOutputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0

      params.setCostTime(costTime)
      println(s"Exec Successful: costTime: ${costTime}")

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(
        s"report/Node2Vec_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }

}
