

package org.apache.spark.graphx.lib

import org.apache.spark.graphx._

import scala.reflect.ClassTag

import scala.collection.mutable.ListBuffer

/**
 * Local clustering coefficient algorithm
 *
 * In a directed graph G=(V, E), we define the neighbourhood N_i of a vertex v_i as
 * N_i={v_j: e_ij \in E or e_ji \in E}
 *
 * The local clustering coefficient C_i of a vertex v_i is then defined as
 * C_i = |{e_jk: v_j, v_k \in N_i, e_jk \in E}| / (K_i * (K_i - 1))
 * where K_i=|N_i| is the number of neighbors of v_i
 *
 * Note that the input graph must have been partitioned using
 * [[org.apache.spark.graphx.Graph#partitionBy]].
 */
object LocalClusteringCoefficient {
  /**
   * Compute the local clustering coefficient for each vertex and
   * return a graph with vertex value representing the local clustering coefficient of that vertex
   *
   * @param graph the graph for which to compute the connected components
   *
   * @return a graph with vertex attributes containing
   *         the local clustering coefficient of that vertex
   *
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(nbrs.length)
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if(nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    val counters = lccCompute(g, nbrSets)

    // count number of neighbors for each vertex
    var nbNumMap = Map[VertexId, Int]()
    nbrSets.collect().foreach { case (vid, nbVal) =>
      nbNumMap += (vid -> nbVal.size)
    }

    // Merge counters with the graph
    g.outerJoinVertices(counters) {
      (vid, _, optCounter: Option[Double]) =>
        val dblCount: Double = optCounter.getOrElse(0)
        val nbNum = nbNumMap(vid)
        if (nbNum > 1) {
          dblCount / (nbNum * (nbNum - 1))
        }
        else {
          0
        }
    }
  }

  def runGlobalClusteringCoefficient[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Double = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues{(vid, nbrs) =>
        val set = new VertexSet(nbrs.length)
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if(nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    val counters = lccCompute(g, nbrSets)
    val openTc = nbrSets.map(f => {
      val degree = f._2.size
      degree * (degree - 1) / 2
    }).sum
    val closeTc = counters.map(_._2).sum() / 2
    println(closeTc)
    println(openTc)
    closeTc / openTc
  }


  def lccCompute[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED], nbrSets: VertexRDD[VertexSet]): VertexRDD[Double] = {
    // join the sets with the graph
    val setGraph: Graph[VertexSet, ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }

    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(et: EdgeContext[VertexSet, ED, Double]){
      assert(et.srcAttr != null)
      assert(et.dstAttr != null)
      val (smallSet, largeSet) = if (et.srcAttr.size < et.dstAttr.size) {
        (et.srcAttr, et.dstAttr)
      } else {
        (et.dstAttr, et.srcAttr)
      }
      val iter = smallSet.iterator
      var buf = 0.0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != et.srcId && vid != et.dstId && largeSet.contains(vid)) {
          buf += 1.0
        }
      }
      et.sendToDst(buf)
      et.sendToSrc(buf)
    }

    // compute the intersection along edges
    setGraph.aggregateMessages(edgeFunc, _ + _)
  }
}