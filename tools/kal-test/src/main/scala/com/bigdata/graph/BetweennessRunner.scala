package com.bigdata.graph

import java.io.{File, FileWriter, InputStreamReader}
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.lib.Betweenness
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class BetweennessConfig extends Serializable{
  @BeanProperty var betweenness: util.HashMap[String, Object] = _
}

class BetweennessParams extends Serializable{
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var computePartitions: Int = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var k: Int = _
  @BeanProperty var p: Float = _
  @BeanProperty var computeTopK = "no"
  @BeanProperty var groundTruthPath: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}



object BetweennessRunner {

  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val isRaw = args(1)
      val partition = args(2).toInt
      val inputPath = args(3)
      val computeTopK = args(4)
      val outputPath = args(5)
      val groundTruthPath = args(6)

      val representer = new Representer
      representer.addClassTag(classOf[BetweennessParams], Tag.MAP)

      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val stream: InputStreamReader = Utils.getStream("conf/graph/betweenness/betweenness.yml")
      val yaml = new Yaml(new Constructor(classOf[BetweennessConfig]), representer, options)
      val description = new TypeDescription(classOf[BetweennessParams])
      yaml.addTypeDescription(description)
      val config: BetweennessConfig = yaml.load(stream).asInstanceOf[BetweennessConfig]
      val paramsMap: util.HashMap[String, Object] =
        config.betweenness
          .get(datasetName)
          .asInstanceOf[util.HashMap[String, Object]]

      val params = new BetweennessParams()

      params.setDatasetName(datasetName)
      params.setInputPath(inputPath)
      params.setK(paramsMap.get("k").toString.toInt)
      params.setP(paramsMap.get("p").toString.toFloat)
      params.setComputeTopK(computeTopK)
      params.setIsRaw(isRaw)
      params.setComputePartitions(partition)
      params.setOutputPath(outputPath)
      params.setSplitGraph(paramsMap.get("splitGraph").toString)
      params.setGroundTruthPath(groundTruthPath)
      params.setAlgorithmName("Betweenness")
      params.setTestcaseType(s"Betweenness_${datasetName}")
      val conf = new SparkConf().setAppName(s"Betweenness_${datasetName}")
      val sc = new SparkContext(conf)

      val startTime = System.currentTimeMillis()

      val edgeRDD = Util.readEdgeListFromHDFS(sc, inputPath, params.getSplitGraph, isWeighted = false, partition)

      val result = Betweenness.run(edgeRDD, params.getK, params.getP)
      Util.saveDataToHDFS(result, ",", outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0

      params.setCostTime(costTime)
      if (computeTopK.equals("yes")) {
        val groundTruthSet = Util.readTopKResultFromHDFS(sc,
          groundTruthPath, ",", partition).collect().toSet
        val resultSet = Util.readTopKResultFromHDFS(sc,
          outputPath, ",", partition).collect().toSet
        val accuracy = groundTruthSet.intersect(resultSet).size.toDouble / groundTruthSet.size

        println(s"Top-K Betweenness Computing Finished. Accuracy ${accuracy}.\nExec Successful: costTime: ${costTime}")
      } else {
        println(s"Exec Successful: costTime: ${costTime}")
      }
      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(
        s"report/Betweenness_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }

}
