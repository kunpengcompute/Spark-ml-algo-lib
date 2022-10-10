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
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.{SparkConf, SparkContext}

class CCParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var partition = new JHashMap[String, Int]
  @BeanProperty var split = new JHashMap[String, String]

  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var curPartition: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object ConnectedComponentsRunner {
  private val RESULT_SPLIT = ","
  private val PARAM_FILEPATH = "conf/graph/cc/cc.yml"

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println(args.mkString(","))
      println("Usage:ConnectedComponentsRunner <dataset_name> <input_path> <output_path> <is_raw> <cpu_name>")
      System.exit(-1)
    }
    val dataset = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val isRaw = args(3)
    val cpuName = args(4)

    val representer = new Representer
    representer.addClassTag(classOf[CCParams], Tag.MAP)
    val options = new DumperOptions
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(new Constructor(classOf[CCParams]), representer, options)
    val description = new TypeDescription(classOf[CCParams])
    yaml.addTypeDescription(description)
    val params = yaml.load(Utils.getStream(PARAM_FILEPATH)).asInstanceOf[CCParams]
    val partition = params.getPartition.get(s"${dataset}_${cpuName}_${isRaw}")
    val split = params.getSplit.get(dataset)
    val appName = if ("yes".equals(isRaw)) {
      s"CC_RAW_${dataset}_${cpuName}"
    } else {
      s"CC_${dataset}_${cpuName}"
    }

    try {
      val sc = new SparkContext(new SparkConf().setAppName(appName))
      val startTime = System.currentTimeMillis()

      val inputRdd = Util.readUndirectDataFromHDFS(sc, inputPath, split, partition).map(x => (x._1.toLong, x._2.toLong))
      val graph = Graph.fromEdgeTuples(inputRdd, 0)
      val result = ConnectedComponents.run(graph).vertices
      Util.saveDataToHDFS(result, RESULT_SPLIT, outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)

      println(s"Exec Successful: connected components costTime: ${costTime}s")

      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setDatasetName(dataset)
      params.setIsRaw(isRaw)
      params.setCurPartition(s"$partition")
      params.setAlgorithmName("CC")
      params.setTestcaseType(appName)

      Utils.checkDirs("report")
      val writer = new FileWriter(s"report/CC_${
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
