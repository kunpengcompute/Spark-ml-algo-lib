// scalastyle:off

package com.bigdata.compare.graph

import java.io.InputStreamReader

import scala.collection.Map
import scala.util.Try
import com.bigdata.utils.Utils
import com.bigdata.graph.{DeepWalkConfig, DeepWalkParams}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.apache.spark.graphx.VertexId
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.representer.Representer
import smile.math.MathEx.cos
import smile.validation.AUC

object DeepWalkVerify {

  def readEdgeListFromHDFS(
                            sc: SparkContext,
                            filePath: String,
                            split: String,
                            partition: Int): RDD[(VertexId, VertexId, Double)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        }
        else {
          val x = line.trim.split(split)
          if (x.length < 2) {
            Iterator.empty
          }
          else {
            var w = x(2).toDouble
            Iterator.single((x(0).toLong, x(1).toLong, w))
          }
        }
      })
  }

  def readUndirectEdgeFromHDFS(
                                sc: SparkContext,
                                filePath: String,
                                split: String,
                                partition: Int): RDD[(Long, Long)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          val x = line.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            val node1 = x(0).toLong
            val node2 = x(1).toLong
            Iterator.single((node1, node2))
          }
        }
      })
  }

  def readNode2VecModel(sc: SparkContext, input: String): RDD[(Long, Vector)] = {
    val rdd: RDD[(Long, Vector)] = sc
      .textFile(input)
      .mapPartitions(it => {
        val regexp = "([0-9]+) \\((.*)\\)".r
        it.map { case regexp(u, emb) => (u.toLong, new DenseVector(emb.split(",")
          .map(_.toDouble)): Vector)
        }
      }).cache()
    rdd
  }

  def get(modelRDD: RDD[(Long, Vector)]): Map[Long, Vector] = {
    modelRDD.collectAsMap()
  }

  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName) = (modelConfSplit(0), modelConfSplit(1))
      val graphPath = args(1)
      val negEdgePath = args(2)
      val embeddingPath = args(3)
      val isRaw = args(4)

      val representer = new Representer
      representer.addClassTag(classOf[DeepWalkParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val stream: InputStreamReader = Utils.getStream("conf/graph/deepwalk/deepwalk.yml")
      val yaml = new Yaml(new Constructor(classOf[DeepWalkConfig]), representer, options)
      val description = new TypeDescription(classOf[DeepWalkParams])
      yaml.addTypeDescription(description)
      val config: DeepWalkConfig = yaml.load(stream).asInstanceOf[DeepWalkConfig]
      val paramsMap =
        config.deepwalk.get(datasetName).get(isRaw match {
          case "no" => "opt"
          case _ => "raw"
        })

      val params = new DeepWalkParams()

      params.setDatasetName(datasetName)
      params.setPartitions(paramsMap.get("partitions").toString.toInt)
      params.setSplitGraph(paramsMap.get("splitGraph").toString)
      params.setAlgorithmName("DeepWalk")
      params.setTestcaseType(s"DeepWalk_${datasetName}")

      val sparkConf =  new SparkConf().setAppName("DeepwalkVerify")
      val sc = SparkContext.getOrCreate(sparkConf)

      val edgesRDD = readEdgeListFromHDFS(sc, graphPath, params.getSplitGraph, params.getPartitions)
      val negativeEdgesRDD = readUndirectEdgeFromHDFS(sc, negEdgePath, ",", params.getPartitions)

      val nvModel: collection.Map[Long, Vector] = get(readNode2VecModel(sc, embeddingPath))

      val nvModelBC = sc.broadcast(nvModel)
      edgesRDD.foreachPartition(_ => nvModelBC.value)

      val positiveEdgesScores: Array[Double] = edgesRDD
        .flatMap({ case (src, dst, weight) =>
          Try(Iterator.single(cos(nvModelBC.value(src).toArray, nvModelBC.value(dst).toArray)))
            .getOrElse(Iterator.empty)
        })
        .filter(score => !score.isInfinity && !score.isNaN)
        .collect()

      val negativeEdgesScores: Array[Double] = negativeEdgesRDD
        .flatMap({ case (src, dst) =>
          Try(Iterator.single(cos(nvModelBC.value(src).toArray, nvModelBC.value(dst).toArray)))
            .getOrElse(Iterator.empty)
        })
        .filter(score => !score.isInfinity && !score.isNaN)
        .collect()

      val truths = Array.fill(positiveEdgesScores.length)(1) ++ Array.fill(negativeEdgesScores.length)(0)
      val auc = AUC.of(truths, (positiveEdgesScores ++ negativeEdgesScores))
      println(s"Link Prediction AUC Score = $auc")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}