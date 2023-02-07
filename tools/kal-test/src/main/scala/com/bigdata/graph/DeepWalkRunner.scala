// scalastyle:off

package com.bigdata.graph

import com.bigdata.utils.Utils
import org.apache.spark.graphx.lib.{DeepWalk, Parameters}
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty
import java.io.{File, FileWriter, InputStreamReader}
import java.util

class DeepWalkConfig extends Serializable {
  @BeanProperty var deepwalk: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class DeepWalkParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var partitions: Int = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var walkLength: Int = _
  @BeanProperty var numWalks: Int = _
  @BeanProperty var iteration: Int = _
  @BeanProperty var dimension: Int = _
  @BeanProperty var windowSize: Int = _
  @BeanProperty var negativeSample: Int = _
}

object DeepWalkRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName) = (modelConfSplit(0), modelConfSplit(1))
      val inputPath = args(1)
      val outputPath = args(2)
      val isRaw = args(3)

      val representer = new Representer
      representer.addClassTag(classOf[DeepWalkParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val stream: InputStreamReader = Utils.getStream("conf/graph/deepwalk/deepwalk.yml")
      val yaml = new Yaml(new Constructor(classOf[DeepWalkConfig]), representer, options)
      val description = new TypeDescription(classOf[DeepWalkParams])
      yaml.addTypeDescription(description)
      val config: DeepWalkConfig = yaml.load(stream).asInstanceOf[DeepWalkConfig]

      val paramsMap =
        config.deepwalk.get(datasetName).get(isRaw match {
          case "no" => "opt"
          case _ => "raw"
        }).asInstanceOf[util.HashMap[String, Object]]

      val params = new DeepWalkParams()

      params.setDatasetName(datasetName)
      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setPartitions(paramsMap.get("partitions").toString.toInt)
      params.setWalkLength(paramsMap.get("walkLength").toString.toInt)
      params.setNumWalks(paramsMap.get("numWalks").toString.toInt)
      params.setNegativeSample(paramsMap.get("negativeSample").toString.toInt)
      params.setIteration(paramsMap.get("iteration").toString.toInt)
      params.setDimension(paramsMap.get("dimension").toString.toInt)
      params.setWindowSize(paramsMap.get("windowSize").toString.toInt)
      params.setSplitGraph(paramsMap.get("splitGraph").toString)
      params.setAlgorithmName("DeepWalk")
      params.setTestcaseType(s"${params.algorithmName}_${datasetName}_${isRaw}")

      val conf = new SparkConf().setAppName(params.testcaseType)
      val sc = new SparkContext(conf)

      val startTime = System.currentTimeMillis()

      val edgeRDD = Util.readCommFromHDFS(sc, inputPath, params.getSplitGraph, params.getPartitions)

      val deepwalkParams = Parameters(params.getWalkLength, params.getNumWalks, params.getIteration, params.getDimension, params.getWindowSize, params.getNegativeSample)

      val deepwalkModel = DeepWalk.run(edgeRDD,deepwalkParams)
      Util.saveNode2VecModel(deepwalkModel, params.getOutputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0

      params.setCostTime(costTime)
      println(s"Exec Successful: costTime: ${costTime}")

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(
        s"report/deepWalk_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
