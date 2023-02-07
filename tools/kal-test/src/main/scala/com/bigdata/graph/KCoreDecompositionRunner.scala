package com.bigdata.graph

import java.io.FileWriter
import java.util.{HashMap => JHashMap}

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.graphx.lib.KCoreDecomposition
import org.apache.spark.{SparkConf, SparkContext}

class KCoreParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var partition = new JHashMap[String, Int]
  @BeanProperty var split = new JHashMap[String, String]
  @BeanProperty var iterNum = new JHashMap[String, Int]

  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var curPartition: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _

}

object KCoreDecompositionRunner {
  private val RESULT_SPLIT = ","
  private val PARAM_FILEPATH = "conf/graph/kcore/kcore.yml"

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println(args.mkString(","))
      println("Usage:KCoreDecompositionRunner <dataset_name> <input_path> <output_path> <is_raw> <cpu_name>")
      System.exit(-1)
    }
    val dataset = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val isRaw = args(3)
    val cpuName = args(4)

    val representer = new Representer
    representer.addClassTag(classOf[KCoreParams], Tag.MAP)
    val options = new DumperOptions
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(new Constructor(classOf[KCoreParams]), representer, options)
    val description = new TypeDescription(classOf[KCoreParams])
    yaml.addTypeDescription(description)
    val params = yaml.load(Utils.getStream(PARAM_FILEPATH)).asInstanceOf[KCoreParams]
    val partition = params.getPartition.get(s"${dataset}_${cpuName}_${isRaw}")
    val split = params.getSplit.get(dataset)
    val appName = s"KCORE_${dataset}_${cpuName}"

    try {
      val sc = new SparkContext(new SparkConf().setAppName(appName))
      val startTime = System.currentTimeMillis()

      val inputRdd = Util.readUndirectDataFromHDFS(sc, inputPath, split, partition)
        .map(x => (x._1.trim.toLong, x._2.trim.toLong))
      val result = KCoreDecomposition.run(inputRdd)
      Util.saveDataToHDFS(result, RESULT_SPLIT, outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)

      println(s"Exec Successful: KCore Decomposition costTime: ${costTime}s")

      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setDatasetName(dataset)
      params.setIsRaw(isRaw)
      params.setCurPartition(s"$partition")
      params.setAlgorithmName("KCore")
      params.setTestcaseType(s"KCore_${dataset}")

      Utils.checkDirs("report")
      val writer = new FileWriter(s"report/KCORE_${
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
