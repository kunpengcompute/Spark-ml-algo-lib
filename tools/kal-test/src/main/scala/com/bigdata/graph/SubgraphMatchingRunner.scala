package com.bigdata.graph

import java.io.FileWriter
import java.util

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.graphx.lib.SubgraphMatching
import org.apache.spark.{SparkConf, SparkContext}

class SubgraphMatchingParams extends Serializable{
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitDataGraph: util.HashMap[String, String] = new util.HashMap[String, String]()
  @BeanProperty var splitQueryGraph: String = _
  @BeanProperty var taskNum: Int = _
  @BeanProperty var resultNum: Int = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var isIdentical: String = _
  @BeanProperty var matchResult: Long = _
}


object SubgraphMatchingRunner {

  def main(args: Array[String]): Unit = {

    try {
      val datasetName = args(0)
      val queryGraphName = args(1)
      val isRaw = args(2)
      val isIdentical = args(3)
      val outputPath = args(4)
      val inputPath = args(5)
      val partitionNum = args(6).toInt
      val taskNum = args(7).toInt
      val queryGraphPath = args(8)
      val testcaseType = s"SGM_${datasetName}_${queryGraphName}_${isIdentical}"

      val representer = new Representer
      representer.addClassTag(classOf[SubgraphMatchingParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[SubgraphMatchingParams]), representer, options)
      val description = new TypeDescription(classOf[SubgraphMatchingParams])
      yaml.addTypeDescription(description)
      val params = yaml.load(Utils.getStream("conf/graph/sgm/sgm.yml")).asInstanceOf[SubgraphMatchingParams]

      val splitDataGraph = params.getSplitDataGraph.get(datasetName)
      val resultNum = params.getResultNum
      val splitQueryGraph = params.getSplitQueryGraph

      params.setAlgorithmName("SGM")
      params.setDatasetName(datasetName)
      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setIsRaw(isRaw)
      params.setNumPartitions(partitionNum)
      params.setTestcaseType(testcaseType)
      params.setIsIdentical(isIdentical)
      params.setTaskNum(taskNum)

      val isIdenticalBool = isIdentical match {
        case "Identical" => true
        case "unIdentical" => false
      }

      val conf = new SparkConf()
        .setAppName(testcaseType)
      val sc = new SparkContext(conf)

      val startTime = System.currentTimeMillis()

      val inputRDD = Util.readUndirectDataFromHDFS(sc, inputPath, splitDataGraph, partitionNum)
        .map(f => (f._1.toLong, f._2.toLong))
      val queryGraphRDD = sc.textFile(queryGraphPath)
      val edgelist: Array[(Long, Long)] = queryGraphRDD.map(line => {
        val strings = line.split(splitQueryGraph)
        (strings(0).toLong, strings(1).toLong)
      }).collect()

      val (numSubgraphs, subgraphs) =
        SubgraphMatching.run(inputRDD, edgelist, taskNum, resultNum, isIdenticalBool)

      params.setMatchResult(numSubgraphs)
      println("total matched results:\t%d".format(numSubgraphs))
      subgraphs.map(x => x.mkString("\t")).saveAsTextFile(outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/SGM_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")

    }
  }
}