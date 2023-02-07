package com.bigdata.graph

import java.util.regex.Pattern

import scala.collection.Map
import scala.reflect.ClassTag

import smile.math.MathEx.cos

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD

object Util {

  def readDirectDataFromHDFS(sc: SparkContext,
                             filePath: String,
                             split: String,
                             partition: Int): RDD[(String, String, String)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          val x = line.split(split)
          if (x.length < 3) {
            Iterator.empty
          } else {
            val node1 = x(0)
            val node2 = x(1)
            val weight = x(2)
            Iterator((node1, node2, weight))
          }
        }
      })
  }

  def readUndirectDataFromHDFS(sc: SparkContext,
                               filePath: String,
                               split: String,
                               partition: Int): RDD[(String, String)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          val x = line.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            val node1 = x(0)
            val node2 = x(1)
            Iterator((node1, node2))
          }
        }
      })
  }

  def readCommFromHDFS(sc: SparkContext,
                       filePath: String,
                       split: String,
                       partition: Int): RDD[(Long, Long)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          var mLine = line
          if (mLine.contains("(") || mLine.contains(")")) {
            mLine = mLine.replaceAll("\\(", "")
            mLine = mLine.replaceAll("\\)", "")
          }
          val x = mLine.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            Iterator((x(0).toLong, x(1).toLong))
          }
        }
      })
  }

  def readGraphFromHDFS(sc: SparkContext,
                        filePath: String,
                        split: String,
                        isWeighted: Boolean,
                        partition: Int): RDD[(Long, Long, Double)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          val x = line.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            var w = 1.0
            if (isWeighted) {
              w = x(2).toDouble
            }
            Iterator((x(0).toLong, x(1).toLong, w))
          }
        }
      })
  }

  def loadLines2EdgesT[ED](
      path: String,
      partNum: Int,
      defaultValue: ED,
      weighted: Boolean,
      split: String = ",")(implicit sc: SparkContext): RDD[Edge[ED]] = {
    var lines = sc.textFile(path, partNum)
    lines =
      if (lines.getNumPartitions > partNum) lines.coalesce(partNum) else lines
    lines.mapPartitions(iterator => {
      val pattern = Pattern.compile(split)
      iterator.flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          val contents = pattern.split(line)
          val srcId = contents(0).trim.toLong
          val dstId = contents(1).trim.toLong
          if (srcId == dstId) {
            Iterator.empty
          } else {
            if (weighted && contents.length >= 3) {
              Iterator(Edge(srcId, dstId, contents(2).asInstanceOf[ED]))
            } else {
              Iterator(Edge(srcId, dstId, defaultValue))
            }
          }
        }
      })
    })
  }

  def readEdgeListFromHDFS(
      sc: SparkContext,
      filePath: String,
      split: String,
      isWeighted: Boolean,
      partition: Int): RDD[(VertexId, VertexId, Double)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          val x = line.trim.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            var w = 1.0
            if (isWeighted && x.length > 2) {
              w = x(2).toDouble
            }
            Iterator.single((x(0).toLong, x(1).toLong, w))
          }
        }
      })
  }

  def readEdgeFileFromHDFS(sc: SparkContext,
                            filePath: String,
                            split: String,
                            partNum: Int): RDD[(Long, Double)] = {
    sc.textFile(filePath).repartition(partNum)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          val x = line.trim.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            Iterator.single((x(0).toLong, x(1).toDouble))
          }
        }
      })
  }

  def readTopKResultFromHDFS(sc: SparkContext,
                             filePath: String,
                             split: String,
                             partition: Int): RDD[VertexId] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#")) {
          Iterator.empty
        } else {
          val x = line.trim.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            Iterator.single(x.head.toLong)
          }
        }
      })
  }

  def readDataFromHDFSForDegree(sc: SparkContext,
                                filePath: String,
                                split: String,
                                partition: Int): RDD[(Long, Long)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#") || line.startsWith("%")) {
          Iterator.empty
        } else {
          val x = line.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            val node1 = x(0)
            val node2 = x(1)
            Iterator((node1.toLong, node2.toLong))
          }
        }
      })
  }
  def readDirectWeightDataFromHDFS(sc: SparkContext,
                                filePath: String,
                                split: String,
                                partition: Int): RDD[(String, String, Double)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#") || line.startsWith("%")) {
          Iterator.empty
        } else {
          val x = line.split(split)
          if (x.length < 3) {
            Iterator.empty
          } else {
            var weight = 1.0
            if(x.length == 3){
              weight = x(2).toDouble
            }
            val node1 = x(0)
            val node2 = x(1)
            Iterator((node1, node2, weight))
          }
        }
      })
  }

  def saveNode2VecModel(modelRDD: RDD[(Long, Vector)], output: String): Unit = {

    modelRDD
      .map{ case (u, vec) => s"$u ${vec.toArray.mkString("(", ",", ")")}"}
      .saveAsTextFile(output)
  }

  def saveDataToHDFS[T: ClassTag](data: RDD[T], filePath: String): Unit = {
    data.saveAsTextFile(filePath)
  }

  def saveDataToHDFS[T: ClassTag, V: ClassTag](data: RDD[(T, V)],
                                               split: String,
                                               filePath: String): Unit = {
    data.map(f => f._1 + split + f._2).saveAsTextFile(filePath)
  }

  def loadSourceVertices(sc: SparkContext, filePath: String): RDD[Long] = {
    sc.textFile(filePath).flatMap { line =>
      if (line.startsWith("#")) {
        Iterator.empty
      } else {
        Iterator(line.trim.toLong)
      }
    }
  }

  def convertToGraphXGraph(edgeRdd: RDD[(Long, Long)]): Graph[Int, Int] = {
    val edges = edgeRdd.map(x => Edge(x._1, x._2, 1))
    val defaultUser = (1)
    val graph = Graph.fromEdges(edges, defaultUser)
    graph
  }

  def buildUnweightedGraph(inputRdd: RDD[(String, String)],
                           isDirected: Boolean,
                           defaultVertex: Int = 0,
                           defaultEdge: Int = 0): Graph[Int, Int] = {
    val edges: RDD[Edge[Int]] = inputRdd.flatMap(f => {
      val src: VertexId = f._1.toLong
      val dst: VertexId = f._2.toLong

      if (isDirected) {
        Iterator(Edge(src, dst, defaultEdge))
      } else {
        if (src < dst) {
          Iterator(Edge(src, dst, defaultEdge))
        } else {
          Iterator(Edge(dst, src, defaultEdge))
        }
      }
    })

    val graph: Graph[Int, Int] = Graph.fromEdges(edges, defaultVertex)
    graph
  }

  def readDataUnwieightedFromHDFS(sc: SparkContext,
                                 filePath: String,
                                 split: String,
                                 partition: Int): RDD[(VertexId, VertexId)] = {
    sc.textFile(filePath, partition)
      .flatMap(line => {
        if (line.startsWith("#") || line.startsWith("%")) {
          Iterator.empty
        } else {
          val x = line.split(split)
          if (x.length < 2) {
            Iterator.empty
          } else {
            val node1 = x(0)
            val node2 = x(1)
            Iterator((node1.toLong, node2.toLong))
          }
        }
      })
  }

  def readEdgeList(sc: SparkContext, filePath: String, split: String, partition: Int): RDD[(Long, Long)] = {
    sc.textFile(filePath, partition).flatMap(line => {
      if (line.startsWith("#")) {
        Iterator.empty
      } else {
        val x = line.trim.split(split)
        if (x.length > 1) {
          val src = x(0).toLong
          val dst = x(1).toLong
          if (src != dst) {
            Iterator.single((src, dst))
          } else {
            Iterator.empty
          }
        } else {
          Iterator.empty
        }
      }
    })
  }

  def get(modelRDD: RDD[(Long, Vector)]): Map[Long, Vector] = {
    modelRDD.collectAsMap()
  }

  def distCos(x: Array[Double], y: Array[Double]): Double = cos(x, y)

  def readNode2VecModel(sc: SparkContext, input: String): RDD[(Long, Vector)] = {
    val rdd: RDD[(Long, Vector)] = sc.textFile(input).mapPartitions(it => {
      val regexp = "([0-9]+) \\((.*)\\)".r
      it.map {
        case regexp(u, emb) => (u.toLong, new DenseVector(emb.split(",")
        .map(_.toDouble)): Vector)
      }
    }).cache()
    rdd
  }
}
