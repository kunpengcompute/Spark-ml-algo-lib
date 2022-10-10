
package com.bigdata.graph

import java.io.FileWriter
import java.util

import scala.beans.BeanProperty
import scala.collection.mutable

import com.bigdata.utils.Utils
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}

import org.apache.spark.SparkConf
import org.apache.spark.graphx.lib.IncPageRank
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

class IncPrConfig extends Serializable {
  @BeanProperty var incpr: util.HashMap[String, Object] = _
}

class IncPrParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var numIter: Int = _
  @BeanProperty var resetProb: Double = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var partNum: Int = _
}

object IncPageRankRunner {
  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val inputPath = args(1)
      val outputPath = args(2)
      val isRaw = args(3)

      val stream = Utils.getStream("conf/graph/incpr/incpr.yml")

      val representer = new Representer
      representer.addClassTag(classOf[IncPrParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[IncPrConfig]), representer, options)
      val description = new TypeDescription(classOf[IncPrParams])
      yaml.addTypeDescription(description)
      val config: IncPrConfig = yaml.load(stream).asInstanceOf[IncPrConfig]
      val paramsMap =
        config.incpr.get(datasetName).asInstanceOf[util.HashMap[String, Object]]

      val params = new IncPrParams()

      // val inputPath = paramsMap.get("inputPath").toString
      // val outputPath = paramsMap.get("outputPath").toString
      val numIter = paramsMap.get("numIter").toString.toInt
      val resetProb = paramsMap.get("resetProb").toString.toDouble
      val partNum = paramsMap.get("partNum").toString.toInt

      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setNumIter(numIter)
      params.setResetProb(resetProb)
      params.setDatasetName(datasetName)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("IncPr")

      println("inputPath: " + inputPath)
      println("outputPath: " + outputPath)

      val appName = s"IncPageRank_${datasetName}"
      params.setTestcaseType(appName)

      val sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("yarn")

      // record start time
      val startTime = System.currentTimeMillis()
      val spark = SparkSession.builder().config(sparkConf).getOrCreate()
      implicit val graph = spark.read
        .orc(inputPath)
        .rdd
        .map(row => (row.getAs[Long]("srcId"),
          row.getAs[Int]("srcStatus"),
          row.getAs[mutable.WrappedArray[Long]]("dstId").toArray[Long],
          row.getAs[mutable.WrappedArray[Int]]("dstStatus").toArray[Int],
          row.getAs[Double]("pr")))
        .persist(StorageLevel.MEMORY_ONLY_SER)
      graph.foreachPartition(f => {})

      val res = IncPageRank.run(graph, partNum, numIter, resetProb)
      res.map(f => f._1 + "\t" + f._2.formatted("%.6f")).saveAsTextFile(outputPath)

      val finishTime = System.currentTimeMillis()
      val costTime = (finishTime - startTime) / 1000
      println(s"IncPageRank Computing Finished. CostTime = $costTime's.")

      params.setCostTime(costTime)

      Utils.checkDirs("report")
      val writer = new FileWriter(
        s"report/IncPR_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

