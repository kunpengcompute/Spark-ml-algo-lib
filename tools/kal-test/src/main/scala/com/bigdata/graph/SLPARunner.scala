// scalastyle:off

package com.bigdata.graph
import com.bigdata.utils.Utils

import org.apache.spark.graphx.lib.{SpearkListenerLabelPropagation, Parameters}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter, InputStreamReader}
import java.util
import scala.beans.BeanProperty

class SLPAConfig extends Serializable {
  @BeanProperty var slpa: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class SLPAParams extends Serializable {
  @BeanProperty var splitGraph: String = _
  @BeanProperty var partitions: Int = _
  @BeanProperty var isWeight: Boolean = _
  @BeanProperty var isDirected: Boolean = _
  @BeanProperty var iterNum: Int = _
  @BeanProperty var threshold: Double = _

  @BeanProperty var outputPath: String = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}


object SLPARunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName, isRaw) = (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val outputPath = args(2)
      val representer = new Representer
      representer.addClassTag(classOf[SLPAParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val stream: InputStreamReader = Utils.getStream("conf/graph/slpa/slpa.yml")
      val yaml = new Yaml(new Constructor(classOf[SLPAConfig]), representer, options)
      val description = new TypeDescription(classOf[SLPAParams])
      yaml.addTypeDescription(description)
      val config: SLPAConfig = yaml.load(stream).asInstanceOf[SLPAConfig]

      val params = new SLPAParams()
      val paramsMap =
        config.slpa.get(isRaw match {
          case "no" => "opt"
          case _ => "raw"
        }).get(datasetName).asInstanceOf[util.HashMap[String, Object]]
      params.setSplitGraph(paramsMap.get("splitGraph").asInstanceOf[String])
      params.setPartitions(paramsMap.get("partitions").asInstanceOf[Int])
      params.setIsWeight(paramsMap.get("isWeight").asInstanceOf[Boolean])
      params.setIsDirected(paramsMap.get("isDirected").asInstanceOf[Boolean])
      params.setIterNum(paramsMap.get("iterNum").asInstanceOf[Int])
      params.setThreshold(paramsMap.get("threshold").asInstanceOf[Double])
      params.setDatasetName(datasetName)
      params.setDataPath(dataPath)
      params.setOutputPath(outputPath)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("SLPA")
      params.setTestcaseType(s"${params.algorithmName}_${datasetName}_${isRaw}")

      val conf = new SparkConf().setAppName(params.testcaseType)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val sc = spark.sparkContext

      val startTime = System.currentTimeMillis()
      val edges = Util.readGraphFromHDFS(sc, params.dataPath, params.splitGraph, params.isWeight, params.partitions)
      edges.foreachPartition{f => {}}

      val slpaGraph = SpearkListenerLabelPropagation.buildGraph(edges, params.isDirected)
      val slpaComm = SpearkListenerLabelPropagation.run(slpaGraph, params.iterNum, params.threshold)
      slpaComm.edges.foreachPartition{f => {}}

      val outPutComm = s"${outputPath}/res"
      val outPutComp = s"${outputPath}/resForComparsion"

      val vertex2Comm = slpaComm.vertices.map(x => (x._1,"[" + x._2.mkString(",") + "]"))
      vertex2Comm.foreachPartition{f => {}}
      vertex2Comm.saveAsTextFile(outPutComm)

      val finishTime = System.currentTimeMillis()
      val costTime = (finishTime - startTime) / 1000

      obtainCommunities(sc, outPutComm, outPutComp)

      params.setCostTime(costTime)
      println(s"Exec Successful: costTime: ${costTime}s")

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

  def obtainCommunities(sc: SparkContext, commPath: String, outPath: String): Unit = {
    val labelIds = sc.textFile(commPath)
      .flatMap(line => {
        val x = line.split(",")
        var tmp = x(0).replace("(","")
        val id = tmp.toLong
        tmp = x(1).replace(")","")
        tmp = tmp.replace("[","")
        tmp = tmp.replace("]","")
        val labels = tmp.split(",")
        val attr = new Array[(Long, Long)](labels.length)
        var i = 0
        while (i < labels.length) {
          attr(i) = (labels(i).toLong, id)
          i = i + 1
        }
        attr.toIterator
      })
    labelIds.groupByKey()
      .map(x => x._2.toArray.mkString(" "))
      .repartition(1)
      .saveAsTextFile(outPath)
  }
}
