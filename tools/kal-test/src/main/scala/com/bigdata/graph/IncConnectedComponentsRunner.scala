// scalastyle:off

package com.bigdata.graph

import java.io.{File, FileWriter, InputStreamReader}
import java.util
import com.bigdata.utils.Utils
import org.apache.spark.graphx.lib.{IncConnectedComponents, Parameters}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class IncCCConfig extends Serializable {
  @BeanProperty var inccc: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class IncCCParams extends Serializable {
  @BeanProperty var splitGraph: String = _
  @BeanProperty var partitions: Int = _

  @BeanProperty var orgCCPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}


object IncConnectedComponentsRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName, isRaw) = (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val outputPath = args(2)
      val orgCCPath = args(3)
      val incGraphPath = args(4)
      val representer = new Representer
      representer.addClassTag(classOf[IncCCParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val stream: InputStreamReader = Utils.getStream("conf/graph/inccc/inccc.yml")
      val yaml = new Yaml(new Constructor(classOf[IncCCConfig]), representer, options)
      val description = new TypeDescription(classOf[IncCCParams])
      yaml.addTypeDescription(description)
      val config: IncCCConfig = yaml.load(stream).asInstanceOf[IncCCConfig]

      val params = new IncCCParams()
      val paramsMap =
        config.inccc.get(isRaw match {
          case "no" => "opt"
          case _ => "raw"
        }).get(datasetName).asInstanceOf[util.HashMap[String, Object]]
      params.setSplitGraph(paramsMap.get("splitGraph").toString)
      params.setPartitions(paramsMap.get("partitions").toString.toInt)
      params.setOrgCCPath(orgCCPath)
      params.setDatasetName(datasetName)
      params.setDataPath(dataPath)
      params.setOutputPath(outputPath)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("IncCC")
      params.setTestcaseType(s"${params.algorithmName}_${datasetName}_${isRaw}")

      val conf = new SparkConf().setAppName(params.testcaseType)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val sc = spark.sparkContext

      val startTime = System.currentTimeMillis()
      val historyCC = Util.readUndirectDataFromHDFS(sc, orgCCPath, params.splitGraph, params.partitions)
        .map(f => (f._1.toLong, f._2.toLong))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      historyCC.foreachPartition(f => {})
      val incGraph = Util.readUndirectDataFromHDFS(sc, incGraphPath, params.splitGraph, params.partitions)
        .map(f => (f._1.toLong, f._2.toLong))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      incGraph.foreachPartition(f => {})
      val res = IncConnectedComponents.run(incGraph, historyCC)
      res.map(f => f._1 + "," + f._2).saveAsTextFile(outputPath)
      val finishTime = System.currentTimeMillis()
      val costTime = (finishTime - startTime) / 1000

      params.setCostTime(costTime)
      println(s"Exec Successful: costTime: ${costTime}")

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(
        s"report/${params.testcaseType}_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
