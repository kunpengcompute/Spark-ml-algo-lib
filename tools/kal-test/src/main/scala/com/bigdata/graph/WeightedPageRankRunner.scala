package com.bigdata.graph

import java.io.{File, FileWriter}
import java.util
import com.bigdata.utils.Utils
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.lib.WeightedPageRank
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class WprConfig extends Serializable {
  @BeanProperty var wpr: util.HashMap[String, Object] = _
}

class WprParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var numIter: Int = _
  @BeanProperty var resetProb: Double = _
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
      val api = args(1)
      val isRaw = args(2)

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
        config.wpr.get(api).asInstanceOf[util.HashMap[String, Object]]

      val params = new WprParams()

      val inputPath = paramsMap.get("inputpath").toString
      val splitGraph = paramsMap.get("splitGraph").toString
      println(s"splitGraph is : $splitGraph")
      val partitionNum = paramsMap.get("partitionNum").toString.toInt
      println(s"partitionNum is : $partitionNum")
      val outputPath = paramsMap.get("outputPath").toString
      val numIter = paramsMap.get("numIter").toString.toInt
      println(s"numIter is : $numIter")
      val resetProb = paramsMap.get("resetProb").toString.toDouble
      println(s"resetProb is : $resetProb")
      val algoTolerance = paramsMap.get("tolerance").toString.toDouble
      println(s"algoTolerance is : $algoTolerance")
      var newOutput = s"${outputPath}_${api}"
      if (isRaw == "yes") {
        newOutput = s"${outputPath}_${api}_raw"
      }

      params.setInputPath(inputPath)
      params.setOutputPath(newOutput)
      params.setSplitGraph(splitGraph)
      params.setNumIter(numIter)
      params.setResetProb(resetProb)
      params.setDatasetName(datasetName)
      params.setPartitionNum(partitionNum)
      params.setApiName(api)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("wpr")

      println("inputPath: " + inputPath)
      println("outputPath: " + newOutput)

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
        org.apache.spark.graphx.lib.WeightedPageRank.run(g, numIter.toInt, 0.15)
      } else {
        org.apache.spark.graphx.lib.WeightedPageRank.runUntilConvergence(g, numIter.toDouble, 0.15)
      }
      val sumresult = result.vertices.values.sum()
      result.vertices.map(f => s"${f._1},${f._2}").saveAsTextFile(outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"WeightedPageRank Computing Finished. sum of WPR result: ${sumresult}")
      println(s"WeightedPageRank Computing Finished. total Time Spend:  ${costTime} s")

      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
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

