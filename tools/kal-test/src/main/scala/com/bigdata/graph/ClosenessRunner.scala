package com.bigdata.graph

import java.io.{File, FileWriter}
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.Closeness
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

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
  @BeanProperty var comparePathWeighted: String = _
  @BeanProperty var comparePathUnweighted: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
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
      val compare = args(7).toBoolean

      val stream = Utils.getStream("conf/graph/closeness/closeness.yml")
      val representer = new Representer
      representer.addClassTag(classOf[ClosenessParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[ClosenessConfig]), representer, options)
      val description = new TypeDescription(classOf[ClosenessParams])
      yaml.addTypeDescription(description)
      val config: ClosenessConfig =
        yaml.load(stream).asInstanceOf[ClosenessConfig]
      val paramsMap =
        config.closeness
          .get(datasetName)
          .asInstanceOf[util.HashMap[String, Object]]

      val params = new ClosenessParams()

      val outputPath = paramsMap.get("outputPath").toString
      val newOutput = s"${outputPath}_${weighted}"
      val splitGraph = paramsMap.get("splitGraph").toString

      params.setInputPath(inputPath)
      params.setOutputPath(newOutput)
      params.setSplitGraph(splitGraph)
      params.setDatasetName(datasetName)
      params.setNumPartitions(numPartitions)
      params.setWeighted(weighted)
      params.setOutputNodeNum(outputNodeNum)
      params.setRatio(ratio)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("Closenness")
      params.setTestcaseType(s"Closenness_${datasetName}_${weighted}")

      println("inputPath: " + inputPath)
      println("outputPath: " + newOutput)

      val weightedBool = weighted match {
        case "weighted"   => true
        case "unweighted" => false
        case _            => throw new Exception("illegal weighted value")
      }

      var appName = s"Closeness_${datasetName}_${weighted}"
      val sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      // record start time
      val startTime = System.currentTimeMillis()

      val edgeRDD =
        Util.readEdgeListFromHDFS(sc,
                                  inputPath,
                                  splitGraph,
                                  weightedBool,
                                  numPartitions)
      val result =
        Closeness.run(edgeRDD, weightedBool, outputNodeNum, ratio)
      Util.saveDataToHDFS(result, ",", newOutput)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(
        s"report/Closeness_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
      if (compare) {
        val groundTruthPath =
          if (weightedBool) { paramsMap.get("comparePathWeighted").toString } else {
            paramsMap.get("comparePathUnweighted").toString
          }

        val groundTruthSet = Util
          .readTopKResultFromHDFS(sc, groundTruthPath, ",", numPartitions)
          .collect()
          .toSet
        val resultSet = Util
          .readTopKResultFromHDFS(sc, newOutput, ",", numPartitions)
          .collect()
          .toSet
        val accuracy = groundTruthSet
          .intersect(resultSet)
          .size
          .toDouble / groundTruthSet.size
        println(s"Accuracy:${accuracy}")
      }
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
