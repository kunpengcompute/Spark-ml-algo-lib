package com.bigdata.graph

import java.io.FileWriter
import java.util

import scala.beans.BeanProperty

import com.bigdata.compare.graph.BetweennessClosenessVerify
import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.graphx.lib.Closeness
import org.apache.spark.{SparkConf, SparkContext}

class ClosenessConfig extends Serializable {
  @BeanProperty var closeness: util.HashMap[String, Object] = _
}

class ClosenessParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var weighted: String = _
  @BeanProperty var outputNodeNum: Int = _
  @BeanProperty var ratio: Double = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var accuracy: Double = _
}

object ClosenessRunner {

  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val numPartitions = args(1).toInt
      val weighted = args(2)
      val outputNodeNum = args(3).toInt
      val ratio = args(4).toDouble
      val isRaw = args(5)
      val inputPath = args(6)
      val outputPath = args(7)
      val split = args(8)
      val groundTruthPath = args(9)
      val check = args(10)

      val params = new ClosenessParams()
      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setSplitGraph(split)
      params.setDatasetName(datasetName)
      params.setNumPartitions(numPartitions)
      params.setWeighted(weighted)
      params.setOutputNodeNum(outputNodeNum)
      params.setRatio(ratio)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("Closenness")
      params.setTestcaseType(s"Closenness_${datasetName}_${weighted}")

      println("inputPath: " + inputPath)
      println("outputPath: " + outputPath)

      val weightedBool = weighted match {
        case "weighted" => true
        case "unweighted" => false
        case _ => throw new Exception("illegal weighted value")
      }

      val appName = s"Closeness_${datasetName}_${weighted}"
      val sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      // record start time
      val startTime = System.currentTimeMillis()

      val edgeRDD =
        Util.readEdgeListFromHDFS(sc,
                                  inputPath,
                                  split,
                                  weightedBool,
                                  numPartitions)
      val result =
        Closeness.run(edgeRDD, weightedBool, outputNodeNum, ratio)
      Util.saveDataToHDFS(result, ",", outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/Closeness_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")

      val representer = new Representer
      representer.addClassTag(classOf[ClosenessParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[ClosenessConfig]), representer, options)
      val description = new TypeDescription(classOf[ClosenessParams])
      yaml.addTypeDescription(description)
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
      if (check.equals("yes")) {
        val acc = BetweennessClosenessVerify.main(Array(groundTruthPath, outputPath, numPartitions.toString))
        params.setAccuracy(acc)
      }
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
