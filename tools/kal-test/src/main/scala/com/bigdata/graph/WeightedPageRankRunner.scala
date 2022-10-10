package com.bigdata.graph

import java.io.FileWriter
import java.util

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.graphx.lib.WeightedPageRank
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

class WprConfig extends Serializable {
  @BeanProperty var wpr: util.HashMap[String, util.HashMap[String, Object]] = _
}

class WprParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var numIter: Int = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var partitionNum: Int = _
  @BeanProperty var apiName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var algoTolerance: String = _
  @BeanProperty var computeType: String = _
}

object WeightedPageRankRunner {
  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val inputPath = args(1)
      val outputPath = args(2)
      val api = args(3)
      val isRaw = args(4)
      val splitGraph = args(5)

      val stream = Utils.getStream("conf/graph/wpr/wpr.yml")

      val representer = new Representer
      representer.addClassTag(classOf[WprParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[WprConfig]), representer, options)
      val description = new TypeDescription(classOf[WprParams])
      yaml.addTypeDescription(description)
      val config: WprConfig = yaml.load(stream).asInstanceOf[WprConfig]
      // 通过算法名字获取对应配置文件的内容
      val paramsMap =
        config.wpr.get(api).get(datasetName).asInstanceOf[util.HashMap[String, Object]]

      val params = new WprParams()

      val partitionNum = paramsMap.get("partitionNum").toString.toInt
      val numIter = paramsMap.get("numIter").toString.toInt
      val algoTolerance = paramsMap.get("tolerance").toString.toDouble

      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setSplitGraph(splitGraph)
      params.setNumIter(numIter)
      params.setDatasetName(datasetName)
      params.setPartitionNum(partitionNum)
      params.setApiName(api)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("wpr")

      var appName = s"WeightedPageRank_${datasetName}_${api}"
      if (isRaw == "yes") {
        appName = s"WeightedPageRank_${datasetName}_${api}_raw"
      }
      params.setTestcaseType(appName)

      val sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      // record start time
      val startTime = System.currentTimeMillis()

      implicit val context = sc
      val edgeRdd = sc.textFile(inputPath, partitionNum).filter(!_.startsWith("%")).map(f => {
        val arr = f.split(splitGraph)
        if(arr.length == 3) {
          Edge(arr(0).toLong, arr(1).toLong, arr(2).toDouble)
        } else {
          Edge(arr(0).toLong, arr(1).toLong, 1.0D)
        }
      })
      val g = Graph.fromEdges(edgeRdd, 1.0)
      val result = if (api.toLowerCase().equals("static")) {
        WeightedPageRank.run(g, numIter, 0.15)
      } else {
        WeightedPageRank.runUntilConvergence(g, algoTolerance, 0.15)
      }
      val sumresult = result.vertices.values.sum()
      result.vertices.map(f => s"${f._1},${f._2}").saveAsTextFile(outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"WeightedPageRank Computing Finished. sum of WPR result: ${sumresult}")
      println(s"WeightedPageRank Computing Finished. total Time Spend:  ${costTime} s")

      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/WPR_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

