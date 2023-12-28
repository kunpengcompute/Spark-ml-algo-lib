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
package com.intel.ssg.bdt.nlp

import breeze.linalg.{Vector => BV}

import scala.collection.mutable.ArrayBuffer

private[nlp] class Node extends Serializable {
  var x = 0
  var y = 0
  var alpha = 0.0
  var beta = 0.0
  var cost = 0.0
  var bestCost = 0.0
  var prev: Option[Node] = None
  var fVector = 0
  val lPath = new ArrayBuffer[Path]()
  val rPath = new ArrayBuffer[Path]()


  /**
    * simplify the log likelihood.
    */
  def logSumExp(x: Double, y: Double, flg: Boolean): Double = {
    val MINUS_LOG_EPSILON = 50.0
    if (flg) y
    else {
      val vMin: Double = math.min(x, y)
      val vMax: Double = math.max(x, y)
      if (vMax > vMin + MINUS_LOG_EPSILON) vMax else vMax + math.log(math.exp(vMin - vMax) + 1.0)
    }
  }

  def calcAlpha(nodes: ArrayBuffer[Node]): Unit = {
    alpha = 0.0
    for(i <- lPath.indices)
      alpha = logSumExp(alpha, lPath(i).cost + nodes(lPath(i).lNode).alpha, i == 0)
    alpha += cost
  }

  def calcBeta(nodes: ArrayBuffer[Node]): Unit = {
    beta = 0.0
    for(i <- rPath.indices)
      beta = logSumExp(beta, rPath(i).cost + nodes(rPath(i).rNode).beta, i == 0)
    beta += cost
  }

  def calExpectation(
      expected: BV[Double],
      Z: Double,
      size: Int,
      featureCache: ArrayBuffer[Int],
      nodes: ArrayBuffer[Node]): Unit = {
    val c: Double = math.exp(alpha + beta -cost - Z)

    var idx: Int = fVector
    while (featureCache(idx) != -1) {
      expected(featureCache(idx) + y) += c
      idx += 1
    }

    for(i <- lPath.indices)
      lPath(i).calExpectation(expected, Z, size, featureCache, nodes)

  }
}

private[nlp] class Path extends Serializable {
  var rNode = 0
  var lNode = 0
  var cost = 0.0
  var fVector = 0

  def calExpectation(
      expected: BV[Double],
      Z: Double,
      size: Int,
      featureCache: ArrayBuffer[Int],
      nodes: ArrayBuffer[Node]): Unit = {
    val c: Double = math.exp(nodes(lNode).alpha + cost + nodes(rNode).beta - Z)
    var idx: Int = fVector

    while (featureCache(idx) != -1) {
      expected(featureCache(idx) + nodes(lNode).y * size + nodes(rNode).y) += c
      idx += 1
    }
  }

  def add(lnd: Int, rnd: Int, nodes: ArrayBuffer[Node]): Unit = {
    lNode = lnd
    rNode = rnd
    nodes(lNode).rPath.append(this)
    nodes(rNode).lPath.append(this)
  }
}
