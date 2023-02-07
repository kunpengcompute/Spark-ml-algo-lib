package com.bigdata.graph

import java.io.FileWriter
import java.util.{HashMap => JHashMap}

import scala.beans.BeanProperty

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.TriangleCount
import org.apache.spark.sql.SparkSession

class TriangleCountParams extends Serializable {
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

object TriangleCountRunner {
  private val TC_PARAM_FILEPATH = "conf/graph/tc/tc.yml"
  private val TC_RESULT_SPLIT = ","

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println(args.mkString(","))
      println("Usage:TriangleCountRunner <dataset_name><input_path><api_name><isRaw>")
      System.exit(-1)
    }
    val dataset = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val api = args(3)
    val isRaw = args(4)

    val representer = new Representer
    representer.addClassTag(classOf[TriangleCountParams], Tag.MAP)
    val options = new DumperOptions
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(new Constructor(classOf[TriangleCountParams]), representer, options)
    val description = new TypeDescription(classOf[TriangleCountParams])
    yaml.addTypeDescription(description)

    val params = yaml.load(Utils.getStream(TC_PARAM_FILEPATH)).asInstanceOf[TriangleCountParams]
    val split = params.getSplit.get(dataset)
    try {
      var appName = s"TC_${api}_${dataset}"
      if (isRaw.equals("yes")) {
        appName = s"TC_RAW_${api}_${dataset}"
      }
      val conf = new SparkConf().setAppName(appName)
      val spark = SparkSession.builder().config(conf).getOrCreate()
      val startTime = System.currentTimeMillis()

      val inputRDD = Util.readUndirectDataFromHDFS(spark.sparkContext, inputPath, split, params.getPartition)
        .flatMap { x =>
          if (x._1.toLong == x._2.toLong) {
            Iterator.empty
          } else if (x._1.toLong > x._2.toLong) {
            Iterator((x._2.toLong, x._1.toLong))
          } else {
            Iterator((x._1.toLong, x._2.toLong))
          }

        }
      val graph = Graph.fromEdgeTuples(inputRDD, 0)
      val result = api match {
        case "preCanonical" => TriangleCount.runPreCanonicalized(graph).vertices.repartition(params.getPartition)
        case _ => TriangleCount.run(graph).vertices.repartition(params.getPartition)
      }
      Util.saveDataToHDFS(result, TC_RESULT_SPLIT, outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setCostTime(costTime)
      params.setDatasetName(dataset)
      params.setApiName(api)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("TriangleCount")
      params.setTestcaseType(appName)

      Utils.checkDirs("report")
      val writer = new FileWriter(s"report/TC_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: triangle count costTime: ${costTime}s")

      spark.stop()
    } catch {
      case e: Throwable =>
        println(s"Exec Failure:${e.getMessage}")
        throw e
    }
  }
}
