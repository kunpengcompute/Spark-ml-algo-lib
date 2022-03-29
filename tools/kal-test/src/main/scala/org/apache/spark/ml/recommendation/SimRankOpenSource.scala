// scalastyle:off
package org.apache.spark.ml.recommendation

import org.apache.spark.{Dependency, ShuffleDependency, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
 * simrank
 *
 */
class SimRankOpenSource() {

  private val PARAM_NAME_DAMP = "damp"
  private val INPUT_NAME_NODES_INFO = "nodes"
  private val PARAM_NAME_MAX_ITERATOR = "maxIterator"

  def execute(input: DataFrame, colNames: (String, String), damp: Double, maxIter: Int): (DataFrame, DataFrame) = {
    import input.sparkSession.implicits._
    val nodes = input.select(colNames._1, colNames._2).distinct().map(one => (one.getString(0), one.getString(1))).rdd.cache()
    val sim = SimRankOpenSource.getSimilarity(nodes, damp, maxIter)

    (sim._1.toDF(colNames._1 + "1", colNames._1 + "2", "similarity"), sim._2.toDF(colNames._2 + "1", colNames._2 + "2", "similarity"))
  }

}

class SimRankGraphOpenSource() {
  /**
   * 创建图的结构
   *
   * @param node2Index 节点idx信息
   * @param nodes      用户商品二部图
   * @return
   */
  def graphStruct(node2Index: RDD[(String, Long)], nodes: RDD[(String, String)]): Graph[String, Int] = {

    val indexedNodes = nodes.join(node2Index).map(r => (r._2._1, r._2._2)).join(node2Index).map(r => (r._2._1, r._2._2))

    val relationShips: RDD[Edge[Int]] = indexedNodes.map { x =>
      val x1 = x._1
      val x2 = x._2
      Edge(x1, x2, 1)
    }
    val users: RDD[(VertexId, String)] = node2Index.map { x =>
      (x._2, x._1)
    }

    val graph = Graph(users, relationShips)
    graph
  }
}

object SimRankOpenSource {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  /**
   * 获取item相似图
   *
   * @param nodes 用户商品节点信息[device_id, goods_id]
   * @param damp  阻尼系数
   */
  def getSimilarity(nodes: RDD[(String, String)],
                    damp: Double, maxIte: Int): (RDD[(String, String, Double)], RDD[(String, String, Double)]) = {
    val itemSet = nodes.map(x => (x._2, "-")).distinct()
    val userSet = nodes.map(x => (x._1, "-")).distinct()
    // 用户商品index化
    val node2IndexArray = (nodes.map(_._1) union nodes.map(_._2)).distinct.collect().zipWithIndex.map(one => (one._1, one._2.toLong))
    val nodesNum = node2IndexArray.length
    val node2Index = nodes.sparkContext.parallelize(node2IndexArray)

    val graph = new SimRankGraphOpenSource().graphStruct(node2Index, nodes)
    val outs = graph.outDegrees.map(x => (x._1, 1 / x._2.toDouble))
    val ins = graph.inDegrees.map(x => (x._1, 1 / x._2.toDouble))
    val rdd_out = graph.outerJoinVertices(outs)((id, _, degin) => (id.toString, degin.getOrElse(0))).triplets.map { x =>
      (x.dstId, x.srcId, x.srcAttr._2.toString.toDouble * x.attr)
    }
    val rdd_int = graph.outerJoinVertices(ins)((id, _, degin) => (id.toString, degin.getOrElse(0))).triplets.map { x =>
      (x.srcId, x.dstId, x.dstAttr._2.toString.toDouble * x.attr)
    }

    val rdd_all = rdd_out.union(rdd_int)

    val blockSize = 2048
    val transferMatrix = new CoordinateMatrix(rdd_all.map { x =>
      MatrixEntry(x._1, x._2, x._3)
    }.repartition(1000)).toBlockMatrix(blockSize, blockSize).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 初始化相似度矩阵
    var S_k = new CoordinateMatrix(nodes.sparkContext.parallelize(0 until nodesNum).map { x =>
      MatrixEntry(x, x, 1.0)
    }.repartition(1000))
    // K+1次迭代相似度矩阵
    var S_kp1 = S_k.entries

    for (i <- 0 until maxIte) {
      S_kp1 = transferMatrix.transpose.multiply(S_k.toBlockMatrix(blockSize, blockSize))
        .multiply(transferMatrix).toCoordinateMatrix().entries.map(entry => {
        if (entry.i == entry.j) {
          MatrixEntry(entry.i, entry.j, 1.0)
        } else {
          MatrixEntry(entry.i, entry.j, entry.value * damp)
        }
      }).persist(StorageLevel.MEMORY_AND_DISK_SER).setName("S_Kp1")
      S_kp1.foreachPartition(_ => {})
      cleanShuffleDependencies(S_kp1.sparkContext, S_kp1.dependencies)
      S_k = new CoordinateMatrix(S_kp1, nodesNum, nodesNum)
    }

    // RDD[(idx, device_id/goods_id)]
    val index2Node = node2Index.map(x => (x._2, x._1))

    val nodeSim = S_kp1.map(x => (x.i, x.j, x.value)).map(x => (x._1, (x._2, x._3)))
      .join(index2Node)
      .map(x => (x._2._1._1, (x._2._1._2, x._2._2)))
      .join(index2Node)
      .map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
    val userSim = nodeSim.join(userSet).map(x => (x._1, x._2._1._1, x._2._1._2)).filter(x => !x._1.equals(x._2))
    val itemSim = nodeSim.join(itemSet).map(x => (x._1, x._2._1._1, x._2._1._2)).filter(x => !x._1.equals(x._2))
    (userSim, itemSim)
  }

  def cleanShuffleDependencies[T](
    sc: SparkContext,
    deps: Seq[Dependency[_]],
    blocking: Boolean = false): Unit = {
    // If there is no reference tracking we skip clean up.
    sc.cleaner.foreach { cleaner =>
      /**
       * Clean the shuffles & all of its parents.
       */
      def cleanEagerly(dep: Dependency[_]): Unit = {
        if (dep.isInstanceOf[ShuffleDependency[_, _, _]]) {
          val shuffleId = dep.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
          cleaner.doCleanupShuffle(shuffleId, blocking)
        }
        val rdd = dep.rdd
        val rddDeps = rdd.dependencies
        if (rdd.getStorageLevel == StorageLevel.NONE && rddDeps != null) {
          rddDeps.foreach(cleanEagerly)
        }
      }
      deps.foreach(cleanEagerly)
    }
  }
}
