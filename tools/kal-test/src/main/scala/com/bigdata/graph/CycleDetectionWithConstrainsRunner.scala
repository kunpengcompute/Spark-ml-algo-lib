package com.bigdata.graph

import java.io.{File, FileWriter}
import java.util.{HashMap => JHashMap}

import com.bigdata.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.lib.CycleDetectionWithConstrains

import scala.beans.BeanProperty
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

class CDParams extends Serializable {
  @BeanProperty var partition = new JHashMap[String, Int]
  @BeanProperty var split = new JHashMap[String, String]
  @BeanProperty var minLoopLen: Int = _
  @BeanProperty var maxLoopLen: Int = _
  @BeanProperty var minRate: Double = _
  @BeanProperty var maxRate: Double = _

  @BeanProperty var datasetName: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object CycleDetectionWithConstrainsRunner {
  private val CD_RESULT_SPLIT = ","
  private val CD_PARAM_FILEPATH = "conf/graph/cd/cd.yml"

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      println(args.mkString(","))
      println("Usage:CycleDetectionWithConstrainsRunner <dataset_name> <input_path> <output_path> <api> <is_raw> <cpu_name>")
      System.exit(-1)
    }
    val dataset = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val api = args(3)
    val isRaw = args(4)
    val cpuName = args(5)

    val representer = new Representer
    representer.addClassTag(classOf[CDParams], Tag.MAP)
    val options = new DumperOptions
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(new Constructor(classOf[CDParams]), representer, options)
    val description = new TypeDescription(classOf[CDParams])
    yaml.addTypeDescription(description)
    val params = yaml.load(Utils.getStream(CD_PARAM_FILEPATH)).asInstanceOf[CDParams]
    val split = params.getSplit.get(dataset)
    val partition = params.getPartition.get(s"${dataset}_${cpuName}_${isRaw}")
    val appName = s"CD_${dataset}_${api}_${cpuName}"
    try {
      val sc = new SparkContext(new SparkConf().setAppName(appName))
      val startTime = System.currentTimeMillis()

      val input = dataset match {
        case "usaRoad" =>
          sc.textFile(inputPath, partition)
            .filter(_.startsWith("a"))
            .flatMap{x =>
              val lines = x.split(split)
              if (lines.length != 4) {
                Iterator.empty
              } else {
                Iterator.single((lines(1).toLong, lines(2).toLong, lines(3).toDouble))
              }

            }
        case _ =>
          sc.textFile(inputPath, partition)
            .filter(!_.startsWith("#"))
            .flatMap{x =>
              val lines = x.split(split)
              if (lines.length != 3) {
                Iterator.empty
              } else {
                Iterator.single((lines(0).toLong, lines(1).toLong, lines(2).toDouble))
              }

            }
      }
      val result = CycleDetectionWithConstrains.run(input, partition, params.minLoopLen,params.maxLoopLen,
        params.minRate,params.maxRate)

      Util.saveDataToHDFS(result.map(_.mkString(CD_RESULT_SPLIT)), outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setCostTime(costTime)
      params.setDatasetName(dataset)
      params.setApiName(api)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("CD")
      params.setTestcaseType(s"CD_${dataset}")
      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/CD_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: cycle detection costTime: ${costTime}s")

      sc.stop()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure:${e.getMessage}")
        throw e
    }
  }
}
