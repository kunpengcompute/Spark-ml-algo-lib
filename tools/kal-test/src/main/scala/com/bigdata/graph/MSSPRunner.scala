package com.bigdata.graph

import java.io.{File, FileWriter}
import java.util

import com.bigdata.utils.Utils
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class MsspConfig extends Serializable {
  @BeanProperty var mssp: util.HashMap[String, Object] = _
}

class MsspParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var sourcePath: String = _
  @BeanProperty var computePartition: Int = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

object MSSPRunner {
  private val ARGS_LEN_RAW = 3

  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val computePartition = args(1).toInt
      var isRaw = "noraw"
      if(args.length == ARGS_LEN_RAW){
        isRaw = args(2)
      }

      val stream = Utils.getStream("conf/graph/mssp/mssp.yml")

      val representer = new Representer
      representer.addClassTag(classOf[MsspParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[MsspConfig]), representer, options)
      val description = new TypeDescription(classOf[MsspParams])
      yaml.addTypeDescription(description)

      val config: MsspConfig = yaml.load(stream).asInstanceOf[MsspConfig]
      val paramsMap = config.mssp.get(datasetName).asInstanceOf[util.HashMap[String, Object]]

      val params = new MsspParams()

      val inputPath = paramsMap.get("inputPath").toString
      val sourcePath = paramsMap.get("sourcePath").toString
      val splitGraph = paramsMap.get("splitGraph").toString
      val outputPath = paramsMap.get("outputPath").toString


      params.setInputPath(inputPath)
      params.setSourcePath(sourcePath)
      params.setComputePartition(computePartition)
      params.setSplitGraph(splitGraph)
      params.setOutputPath(outputPath)
      params.setDatasetName(datasetName)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("Mssp")

      println("inputPath: " + inputPath)
      println("sourcePath: " + sourcePath)
      println("computePartition: " + computePartition)
      println("splitGraph: " + splitGraph)
      println("outputPath: " + outputPath)
      println("datasetName: " + datasetName)
      println("isRaw: " + isRaw)
      var appName = s"MSSP_${datasetName}"
      if (isRaw.equals("yes")){
        appName = s"MSSP_RAW_${datasetName}"
      }

      params.setTestcaseType(appName)

      val sparkConf = new SparkConf().setAppName(appName).setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      // record start time
      val startTime = System.currentTimeMillis()

      val edgeRDD = Util.readUndirectDataFromHDFS(sc, inputPath, splitGraph, computePartition)
        .map(f => (f._1.trim.toLong, f._2.trim.toLong)).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val inputGraph = Util.convertToGraphXGraph(edgeRDD).persist()
      val source = Util.loadSourceVertices(sc, sourcePath)

      val res = ShortestPaths.run(inputGraph, source.collect.toSeq).vertices
      res.map(f => {
        var s = f._1.toString + ":"
          f._2.foreach(x => s = s + "(" + x._1.toString + "," + x._2.toString + ")")
        s
      }).saveAsTextFile(outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println("Short Path result = true, and costTime = " + costTime + "s")

      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/MSSP_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
