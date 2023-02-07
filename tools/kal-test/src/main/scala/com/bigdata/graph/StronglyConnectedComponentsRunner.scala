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
import org.apache.spark.graphx.lib.StronglyConnectedComponents
import org.apache.spark.{SparkConf, SparkContext}

class SCCParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var partition: Int = _
  @BeanProperty var split: JHashMap[String, String] = new JHashMap[String, String]

  @BeanProperty var datasetName: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object StronglyConnectedComponentsRunner {
  private val SCC_RESULT_SPLIT = ","
  private val SCC_NUM_ITER = Integer.MAX_VALUE

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      println(args.mkString(","))
      println("Usage:StronglyConnectedComponents <dataset_name> <input_path> <output_path> <api> <is_raw> <cpu_name>")
      System.exit(-1)
    }
    val dataset = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val api = args(3)
    val isRaw = args(4)
    val cpuName = args(5)
    val partNum = args(6).toInt
    val paramFilepath = "conf/graph/scc/scc.yml"

    val representer = new Representer
    representer.addClassTag(classOf[SCCParams], Tag.MAP)
    val options = new DumperOptions
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(new Constructor(classOf[SCCParams]), representer, options)
    val description = new TypeDescription(classOf[SCCParams])
    yaml.addTypeDescription(description)
    val params = yaml.load(Utils.getStream(paramFilepath)).asInstanceOf[SCCParams]
    val split = params.getSplit.get(dataset)
    val appName = s"SCC_${dataset}_${api}_${cpuName}"
    try {
      val sc = new SparkContext(new SparkConf().setAppName(appName))
      val startTime = System.currentTimeMillis()

      val input = Util.readUndirectDataFromHDFS(sc, inputPath, split, partNum)
        .flatMap(x => Iterator((x._1.toLong, x._2.toLong)))
      val graph = Graph.fromEdgeTuples(input, 0)
      val result = StronglyConnectedComponents.run(graph, SCC_NUM_ITER)
      Util.saveDataToHDFS(result.vertices, SCC_RESULT_SPLIT, outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setCostTime(costTime)
      params.setDatasetName(dataset)
      params.setApiName(api)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("SCC")
      params.setTestcaseType(s"SCC_${dataset}")

      Utils.checkDirs("report")
      val writer = new FileWriter(s"report/SCC_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: strongly connected component costTime: ${costTime}s")

      sc.stop()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure:${e.getMessage}")
        throw e
    }
  }
}
