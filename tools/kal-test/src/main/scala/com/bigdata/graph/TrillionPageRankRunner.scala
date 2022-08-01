package com.bigdata.graph

import java.io.{File, FileWriter}
import java.util
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, TripletFields, VertexId}
import org.apache.spark.graphx.lib.TrillionPageRank
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty
import com.bigdata.utils.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class TrillionPageRankConfig extends Serializable {
  @BeanProperty var tpr: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class TrillionPageRankParams extends Serializable {
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var splitGraph: String = _
  @BeanProperty var numIter: Int = _
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var resetProb: Double = _
  @BeanProperty var isOnlySrc: Boolean = _

  @BeanProperty var costTime: Double = _
  @BeanProperty var isRaw: String = _
}

object TrillionPageRankRunner {
  def main(args: Array[String]): Unit = {
    try {
      val datasetName = args(0)
      val inputPath = args(1)
      val outputPath = args(2)
      val isRaw = args(3)

      val stream = Utils.getStream("conf/graph/tpr/tpr.yml")

      val representer = new Representer
      representer.addClassTag(classOf[TrillionPageRankParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[TrillionPageRankConfig]), representer, options)
      val description = new TypeDescription(classOf[TrillionPageRankParams])
      yaml.addTypeDescription(description)
      val config: TrillionPageRankConfig = yaml.load(stream).asInstanceOf[TrillionPageRankConfig]
      val paramsMap =
        config.tpr.get(datasetName).get(isRaw match {
        case "no" => "opt"
        case _ => "raw"
      }).asInstanceOf[util.HashMap[String, Object]]

      val params = new TrillionPageRankParams()

      params.setInputPath(inputPath)
      params.setOutputPath(outputPath)
      params.setIsRaw(isRaw)
      params.setNumPartitions(paramsMap.get("numPartitions").asInstanceOf[Int])
      params.setSplitGraph(paramsMap.get("splitGraph").asInstanceOf[String])
      params.setNumIter(paramsMap.get("numIter").asInstanceOf[Int])
      params.setResetProb(paramsMap.get("resetProb").asInstanceOf[Double])
      params.setIsOnlySrc(paramsMap.get("isOnlySrc").asInstanceOf[Boolean])

      println("inputPath: " + inputPath)
      println("outputPath: " + outputPath)

      val sparkConf = new SparkConf().setMaster("yarn")
      val sc = new SparkContext(sparkConf)

      // record start time
      val startTime = System.currentTimeMillis()
      val data = sc.textFile(inputPath, params.numPartitions).map(f => {
        val urls = f.split(params.splitGraph)
        (urls(0).split("_")(0).toLong, (urls(0).split("_")(1).toDouble, urls.drop(1).map(_.toLong)))
      })

      val attr = isRaw match {
        case "no" => TrillionPageRank.run(data, params.numPartitions, params.numIter, params.resetProb, params.isOnlySrc)
        case _ => openCompute(data, params.numPartitions, params.numIter, params.resetProb)
      }
      attr.map(i => i._1 + "\t" + i._2.formatted("%.6f")).saveAsTextFile(params.outputPath)

      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println("TrillionPageRank costTime = " + costTime + "s")

      params.setCostTime(costTime)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(
        s"report/TPR_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)

      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }

  private def openCompute(
     edge: RDD[(Long, (Double, Array[Long]))],
     part: Int,
     numIter: Int,
     resetProb: Double): RDD[(Long, Double)] = {
    val flatEdges = edge.partitionBy(new HashPartitioner(part)).flatMap(f => f._2._2.map(x => (f._1, x)))
    val graph = Graph.fromEdgeTuples(flatEdges, 0, edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)

    runWithOptions(graph, numIter, resetProb).vertices
  }

  private def runWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      numIter: Int,
      resetProb: Double = 0.15,
      srcId: Option[VertexId] =  None): Graph[Double, Double] = {
    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices { (id, attr) =>
        if (!(id != src && personalized)) 1.0 else 0.0
      }

    def delta(u: VertexId, v: VertexId): Double = {
      if (u == v) 1.0 else 0.0
    }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      val rankUpdates = rankGraph.aggregateMessages[Double] (
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)
      prevRankGraph = rankGraph
      val rPrb = if (personalized) {
        (src: VertexId, id: VertexId) => resetProb * delta(src, id)
      } else {
        (src: VertexId, id: VertexId) => resetProb
      }

      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (id, oldRank, msgSumOpt) => rPrb(src, id) + (1.0 - resetProb) * msgSumOpt.getOrElse(0.0)
      }.cache()
      rankGraph.edges.foreachPartition(x => {})
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }
    rankGraph
  }
}
