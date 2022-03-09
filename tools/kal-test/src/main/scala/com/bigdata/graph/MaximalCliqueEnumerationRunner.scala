package com.bigdata.graph

import java.io.{File, FileWriter}
import java.util.{HashMap => JHashMap}

import com.bigdata.utils.Utils
import org.apache.spark.graphx.lib.MaximalCliqueEnumeration

import scala.beans.BeanProperty
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

class MaximalCliqueEnumerationParams extends Serializable {
  @BeanProperty var partition: JHashMap[String, Int] = new JHashMap[String, Int]()
  @BeanProperty var minK: Int = _
  @BeanProperty var maxDegree: Int = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var costTime: Double = _
  @BeanProperty var split: JHashMap[String, String] = new JHashMap[String, String]
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object MaximalCliqueEnumerationRunner {
  private val MCE_PARAM_FILEPATH = "conf/graph/mce/mce.yml"
  private val MCE_OUTPUT_SPLIT = ","

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage:MaxcliqueEnumerationRunner <dataset-name><input-path><output-path>")
      System.exit(-1)
    }
    val dataset = args(0)
    val inputPath = args(1)
    val outputPath = args(2)

    val representer = new Representer
    representer.addClassTag(classOf[MaximalCliqueEnumerationParams], Tag.MAP)
    val options = new DumperOptions
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(new Constructor(classOf[MaximalCliqueEnumerationParams]), representer, options)
    val description = new TypeDescription(classOf[MaximalCliqueEnumerationParams])
    yaml.addTypeDescription(description)
    val params = yaml.load(Utils.getStream(MCE_PARAM_FILEPATH)).asInstanceOf[MaximalCliqueEnumerationParams]
    val split = params.getSplit.get(dataset)
    val partition = params.getPartition.get(dataset)
    val mapInfoPath = s"${outputPath}/map_info"
    val cliquePath = s"${outputPath}/clique_info"

    try {
      val spark = SparkSession.builder().getOrCreate()
      val startTime = System.currentTimeMillis()

      println(s"##start to run test.params:${inputPath},${split},${partition}")
      val inputRdd = Util.readUndirectDataFromHDFS(spark.sparkContext, inputPath, split, partition)
      println(s"##start to run.params:${params.getMinK},${params.getMaxDegree},${partition}")
      val result = MaximalCliqueEnumeration.run(inputRdd, params.getMinK, params.getMaxDegree, partition)
      Util.saveDataToHDFS(result._1, MCE_OUTPUT_SPLIT, mapInfoPath)
      Util.saveDataToHDFS(result._2, MCE_OUTPUT_SPLIT, cliquePath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setDatasetName(dataset)
      params.setCostTime(costTime)
      params.setAlgorithmName("MaximalCliqueEnumeration")
      params.setTestcaseType(s"MaximalCliqueEnumeration_${dataset}")
      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/MCE_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful:maximal clique detection costTime:${costTime}s")
      spark.stop()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure:${e.getMessage}")
        throw e
    }
  }

}
