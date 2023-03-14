/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:off
package org.apache.spark.nlp

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV, Vector => BV}

private[nlp] trait Mode

private[nlp] case object LearnMode extends Mode

private[nlp] case object TestMode extends Mode

private[nlp] case class QueueElement(node : Node, fx : Double, gx : Double, next : QueueElement)

private[nlp] class Tagger (
    ySize: Int,
    mode: Mode) extends Serializable {
  var nBest = 0
  var cost = 0.0
  var Z = 0.0
  var obj = 0.0
  var costFactor = 1.0
  val x = new ArrayBuffer[Array[String]]()
  val nodes  = new ArrayBuffer[Node]()
  val answer = new ArrayBuffer[Int]()
  val result = new ArrayBuffer[Int]()
  val featureCache = new ArrayBuffer[Int]()
  val featureCacheIndex = new ArrayBuffer[Int]()
  val probMatrix = new ArrayBuffer[Double]()
  var seqProb = 0.0
  lazy val topN = ArrayBuffer.empty[Array[Int]]
  lazy val topNResult = ArrayBuffer.empty[Int]
  lazy val probN = ArrayBuffer.empty[Double]
  lazy val agenda = mutable.PriorityQueue.empty[QueueElement] (
    Ordering.by((_:QueueElement).fx).reverse
  )

  def setCostFactor(costFactor: Double) = {
    this.costFactor = costFactor
    this
  }

  def setNBest(nBest: Int) = {
    this.nBest = nBest
    this
  }

  def read(lines: Sequence, feature_idx: FeatureIndex): Unit = {
    lines.toArray.foreach{ t =>
      mode match {
      case LearnMode =>
        for (y <- feature_idx.labels if y.equals(t.label))
          answer.append(feature_idx.labels.indexOf(y))
        x.append(t.tags)
      case TestMode =>
        x.append(t.tags)
        answer.append(0)
      }
      result.append(0)
    }
  }

  /**
   * Set node relationship and its feature index.
   * Node represents a token.
   */
  def rebuildFeatures(): Unit = {

    nodes ++= Array.fill(x.length * ySize)(new Node)
    nodes.zipWithIndex.foreach{ case(n, index) =>
      n.x = index / ySize
      n.y = index - n.x * ySize
      n.fVector = featureCacheIndex(n.x)
    }

    nodes.filter(_.x > 0).foreach{ n =>
      val paths = Array.fill(ySize)(new Path)
      paths.zipWithIndex.foreach { case(p, indexP) =>
        p.fVector = featureCacheIndex(n.x + x.length - 1)
        p.add((n.x - 1) * ySize + n.y, n.x * ySize + indexP, nodes)
      }
    }
  }

  /**
   * Calculate the expectation of each node
   */
  def forwardBackward(): Unit = {
    nodes.foreach(_.calcAlpha(nodes))
    nodes.reverse.foreach(_.calcBeta(nodes))
    Z = 0.0
    nodes.filter(_.x == 0).foreach(n => Z = n.logSumExp(Z, n.beta, n.y == 0))
  }

  /**
   * Get the max expectation in the nodes and predicts the most likely label
   */
  def viterbi(): Unit = {
    var bestCost = Double.MinValue
    var best: Option[Node] = None

    nodes.foreach { n =>
      bestCost = Double.MinValue
      best = None
      n.lPath.foreach { p =>
        val cost = nodes(p.lNode).bestCost + p.cost + n.cost
        if (cost > bestCost) {
          bestCost = cost
          best = Some(nodes(p.lNode))
        }
      }
      n.prev = best
      best match {
        case None =>
          n.bestCost = n.cost
        case _ =>
          n.bestCost = bestCost
      }
    }

    var nd: Option[Node] = Some(nodes.filter(_.x == x.length - 1).max(Ordering.by((_:Node).bestCost)))

    while (nd.isDefined) {
      result.update(nd.get.x, nd.get.y)
      nd = nd.get.prev
    }

    cost = - nodes((x.length - 1)*ySize + result.last).bestCost
  }

  def gradient(expected: BV[Double], alpha: BDV[Double]): Double = {

    buildLattice(alpha)
    forwardBackward()

    nodes.foreach(_.calExpectation(expected, Z, ySize, featureCache, nodes))

    var s: Double = 0.0
    for (i <- x.indices) {
      var rIdx = nodes(i * ySize + answer(i)).fVector
      while (featureCache(rIdx) != -1) {
        expected(featureCache(rIdx) + answer(i)) -= 1.0
        rIdx += 1
      }
      s += nodes(i * ySize + answer(i)).cost
      var j = 0
      while (j < nodes(i * ySize + answer(i)).lPath.length) {
        val lNode = nodes(nodes(i * ySize + answer(i)).lPath(j).lNode)
        val rNode = nodes(nodes(i * ySize + answer(i)).lPath(j).rNode)
        val lPath = nodes(i * ySize + answer(i)).lPath(j)
        if (lNode.y == answer(lNode.x)) {
          rIdx = lPath.fVector
          while (featureCache(rIdx) != -1) {
            expected(featureCache(rIdx) + lNode.y * ySize + rNode.y) -= 1.0
            rIdx += 1
          }
          s += lPath.cost
        }
        j += 1
      }
    }

    viterbi()
    clear()
    Z - s
  }

  def probCalculate(): Unit ={
    probMatrix  ++= Array.fill(x.length * ySize)(0.0)
    var idx :Int = 0
    nodes.foreach{ n =>
      idx = n.x * ySize + n.y
      probMatrix(idx) = Math.exp(n.alpha + n.beta - n.cost - Z)
    }
    this.seqProb = Math.exp(- cost - Z)

  }

  def clear(): Unit = {
    nodes foreach{ n =>
      n.lPath.clear()
      n.rPath.clear()
    }
    nodes.clear()
  }

  def parse(alpha: BDV[Double], mode: Option[VerboseMode]): Unit = {
    buildLattice(alpha)
    if (nBest > 0 || mode.isDefined) {
      forwardBackward()
      viterbi()
      probCalculate()
    }
    else
      viterbi()
    if(nBest > 0) {
      //initialize nBest
      if(agenda.nonEmpty) agenda.clear()
      nodes.slice((x.size - 1) * ySize, x.size * ySize - 1)
        .foreach(n => agenda += QueueElement(n, - n.bestCost, - n.cost, null))
      //find nBest
      for(i <- 0 until this.nBest) {
        topNResult.clear()
        if(!nextNode)
          return
        probN.append(Math.exp(- cost - Z))
        topN.append(topNResult.toArray)
      }
    }
  }

  def buildLattice(alpha: BDV[Double]): Unit = {

    rebuildFeatures()
    nodes.foreach { n =>
      val nn = calcCost(n, alpha)
      nn.lPath.foreach(calcCost(_, alpha))
      nn
    }
  }

  def calcCost(n: Node, alpha: BDV[Double]): Node = {
    var cd: Double = 0.0
    var idx: Int = n.fVector
    n.cost = 0.0

    while (featureCache(idx) != -1) {
      cd += alpha(featureCache(idx) + n.y)
      n.cost = cd * costFactor
      idx += 1
    }

    n
  }

  def calcCost(p: Path, alpha: BDV[Double]): Path = {
    var cd: Double = 0.0
    var idx: Int = p.fVector
    p.cost = 0.0

    while (featureCache(idx) != -1) {
      cd += alpha(featureCache(idx) +
        nodes(p.lNode).y * ySize + nodes(p.rNode).y)
      p.cost = cd * costFactor
      idx += 1
    }

    p
  }

  def nextNode: Boolean = {
    var top: QueueElement = null
    var rNode: Node = null
    while(agenda.nonEmpty) {
      top = agenda.dequeue()
      rNode = top.node
      if(rNode.x == 0) {
        var n: QueueElement = top
        for(i <- x.indices) {
          topNResult.append(n.node.y)
          n = n.next
        }
        cost = top.gx
        return true
      }
      rNode.lPath.foreach { p =>
        val gx = -nodes(p.lNode).cost - p.cost + top.gx
        val fx = - nodes(p.lNode).bestCost - p.cost + top.gx
        agenda  += QueueElement(nodes(p.lNode), fx, gx, top)
      }
    }
    false
  }
}
