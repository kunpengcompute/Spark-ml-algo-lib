package com.bigdata.graph

import java.io.FileWriter
import java.util.{HashMap => JHashMap}

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.lib.{LabelPropagation, Modularity}
import org.apache.spark.{SparkConf, SparkContext}

class LPAParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var partition = new JHashMap[String, Int]
  @BeanProperty var partitionNum: Int = _
  @BeanProperty var split: String = _
  @BeanProperty var maxSteps: Int = _

  @BeanProperty var datasetName: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var modularity: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object LabelPropagationRunner {
  private val RESULT_SPLIT = ","
  private val PARAM_FILEPATH = "conf/graph/lpa/lpa.yml"

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      println(args.mkString(","))
      println("Usage:LabelPropagationRunner <dataset_name> <input_path> <output_path> <api> <is_raw> <cpu_name>")
      System.exit(-1)
    }
    val dataset = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val api = args(3)
    val isRaw = args(4)
    val cpuName = args(5)

    val representer = new Representer
    representer.addClassTag(classOf[LPAParams], Tag.MAP)
    val options = new DumperOptions
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(new Constructor(classOf[LPAParams]), representer, options)
    val description = new TypeDescription(classOf[LPAParams])
    yaml.addTypeDescription(description)
    val params = yaml.load(Utils.getStream(PARAM_FILEPATH)).asInstanceOf[LPAParams]
    val partition = params.getPartition.get(s"${dataset}_${cpuName}")
    val appName = s"LPA_${dataset}_${api}_${cpuName}"
    try {
      val conf = new SparkConf().setAppName(appName)
      if ("runConvergence".equals(api)) {
        conf.set("spark.boostkit.graph.lpa.convergence", "true")
      }
      val sc = new SparkContext(conf)
      val startTime = System.currentTimeMillis()

      val input = Util.readUndirectDataFromHDFS(sc, inputPath, params.getSplit, partition)
        .flatMap(x => Iterator((x._1.toLong, x._2.toLong)))
      val inputGraph =
      if ("yes".equals(isRaw)) {
        Graph.fromEdgeTuples(input, 0).partitionBy(EdgePartition2D)
      } else {
        Graph.fromEdgeTuples(input, 0)
      }
      val result = LabelPropagation.run(inputGraph, params.maxSteps).vertices
      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      Util.saveDataToHDFS(result, RESULT_SPLIT, outputPath)
      params.setCostTime(costTime)
      println(s"Exec Successful: label propagation costTime: ${costTime}s")

      if ("no".equals(isRaw)) {
        val nodes = Util.readCommFromHDFS(sc, outputPath, RESULT_SPLIT, partition)
        val edges = Util.readGraphFromHDFS(sc, inputPath, params.getSplit, false, partition)
        val modularity = Modularity.run(nodes, edges, false, partition)
        params.setModularity(modularity)
        println(s"Modularity: ${modularity}.")
      }
      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setPartitionNum(partition)
      params.setDatasetName(dataset)
      params.setApiName(api)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("LPA")
      params.setTestcaseType(appName)

      Utils.checkDirs("report")
      val writer = new FileWriter(s"report/LPA_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)

      sc.stop()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure:${e.getMessage}")
        throw e
    }
  }
}
