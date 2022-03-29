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

package org.apache.spark.mllib.clustering

import scala.util.Random

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * An utility object to run K-means locally. This is private to the ML package because it's used
 * in the initialization of KMeans but not meant to be publicly exposed.
 */
private[mllib] object LocalKMeansX extends Logging {

  /**
   * Run K-means++ on the weighted point set `points`. This first does the K-means++
   * initialization procedure and then rounds of Lloyd's algorithm.
   */
  def kMeansPlusPlus(
      seed: Int,
      points: Array[VectorWithNorm],
      weights: Array[Double],
      k: Int,
      maxIterations: Int
  ): Array[VectorWithNorm] = {
    val DEFAULT_PAR_LEVEL = 100
    val rand = new Random(seed)
    val dimensions = points(0).vector.size
    val centers = new Array[VectorWithNorm](k)

    // Initialize centers by sampling using the k-means++ procedure.
    centers(0) = pickWeighted(rand, points, weights).toDense
    var costArray = points.map(KMeans.fastSquaredDistance(_, centers(0)))

    var customParLevel = DEFAULT_PAR_LEVEL
    try{
      customParLevel = SparkSession.builder().getOrCreate()
        .sparkContext.getConf.getInt("spark.boostkit.Kmeans.parLevel",
        DEFAULT_PAR_LEVEL)
      if (customParLevel < 1) {
        throw new Exception
      }
    }
    catch {
      case x: Exception =>
        throw new Exception("'spark.boostkit.Kmeans.parLevel' value is invalid")
    }
    val customForkJoinPool = new scala.concurrent.forkjoin.ForkJoinPool(customParLevel)
    val customTaskSupport = new scala.collection.parallel.ForkJoinTaskSupport(customForkJoinPool)

    for (i <- 1 until k) {
      val sum = costArray.zip(weights).map(p => p._1 * p._2).sum
      val r = rand.nextDouble() * sum
      var cumulativeScore = 0.0
      var j = 0
      while (j < points.length && cumulativeScore < r) {
        cumulativeScore += weights(j) * costArray(j)
        j += 1
      }
      if (j == 0) {
        logWarning("kMeansPlusPlus initialization ran out of distinct points for centers." +
          s" Using duplicate point for center k = $i.")
        centers(i) = points(0).toDense
      } else {
        centers(i) = points(j - 1).toDense
      }

      val costArrayPar = points.zipWithIndex.par
      costArrayPar.tasksupport = customTaskSupport
      costArray = costArrayPar.map{
        t =>
          math.min(KMeans.fastSquaredDistance(points(t._2), centers(i)), costArray(t._2))
      }.toArray
    }

    // Run up to maxIterations iterations of Lloyd's algorithm
    val oldClosest = Array.fill(points.length)(-1)
    var iteration = 0
    var moved = true
    while (moved && iteration < maxIterations) {
      moved = false
      val counts = Array.fill(k)(0.0)
      val sums = Array.fill(k)(Vectors.zeros(dimensions))

      val assignPar = points.par
      assignPar.tasksupport = customTaskSupport
      val assign = assignPar.map{
        case point =>
          val index = KMeans.findClosest(centers, point)._1
          (point, index)
      }.toArray

      var idx = 0
      while(idx < assign.size) {
        val index = assign(idx)._2
        if (index != oldClosest(idx)) {
          moved = true
          oldClosest(idx) = index
        }
        axpy(weights(idx), assign(idx)._1.vector, sums(index))
        counts(index) += weights(idx)
        idx += 1
      }

      // Update centers
      var j = 0
      while (j < k) {
        if (counts(j) == 0.0) {
          // Assign center to a random point
          centers(j) = points(rand.nextInt(points.length)).toDense
        } else {
          scal(1.0 / counts(j), sums(j))
          centers(j) = new VectorWithNorm(sums(j))
        }
        j += 1
      }
      iteration += 1
    }

    if (iteration == maxIterations) {
      logInfo(s"Local KMeans++ reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"Local KMeans++ converged in $iteration iterations.")
    }
    centers
  }

  private def pickWeighted[T](rand: Random, data: Array[T], weights: Array[Double]): T = {
    val r = rand.nextDouble() * weights.sum
    var i = 0
    var curWeight = 0.0
    while (i < data.length && curWeight < r) {
      curWeight += weights(i)
      i += 1
    }
    data(i - 1)
  }
}
