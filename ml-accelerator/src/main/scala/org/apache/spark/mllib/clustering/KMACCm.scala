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

import java.util.Date

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD


object KMACCm {
  val DEFAULT_SAMPLE_RATE = 0.05

  def generateNewCenters(
      data: RDD[VectorWithNorm],
      bcs: Broadcast[Array[Double]],
      bcCenters: Broadcast[Array[VectorWithNorm]]): Map[Int, VectorWithNorm] = {
    val newCenters = data
      .mapPartitions { points =>
        val thisS = bcs.value
        val thisCenters = bcCenters.value
        val dims = thisCenters.head.vector.size
        val sums = Array.fill(thisCenters.length)(Vectors.zeros(dims))
        val counts = Array.fill(thisCenters.length)(0L)

        points.foreach { point =>
          val (bestCenter, cost) = KmeansUtil.findClosest(thisCenters, point, thisS)
          val sum = sums(bestCenter)
          axpy(1.0, point.vector, sum)
          counts(bestCenter) += 1
        }
        counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator
      }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
      axpy(1.0, sum2, sum1)
      (sum1, count1 + count2)
    }.mapValues { case (sum, count) =>
      scal(1.0 / count, sum)
      new VectorWithNorm(sum)
    }.collectAsMap()
    newCenters.toMap
  }

  def compute(
      data: RDD[VectorWithNorm],
      centers: Array[VectorWithNorm],
      maxIterations: Int,
      epsilon: Double,
      enableMiniBatch: Boolean): Unit = {
    var converged = false
    var iteration = 0
    val cl = centers.length
    val p = Array.fill(cl)(0.0)
    val sc = data.sparkContext
    var sampleRate = DEFAULT_SAMPLE_RATE
    val temp = data.sparkContext.getConf.getOption("spark.sophon.Kmeans.sampleRate")
    try {
      sampleRate = temp.get.toDouble
    }
    catch {
      case x: Exception =>
        sampleRate = DEFAULT_SAMPLE_RATE
    }

    // println(sampleRate)
    while (iteration < maxIterations && !converged) {
      val s = Array.fill(cl * cl)(0.0)
      KmeansUtil.generateDisMatrix(centers, s)
      val bcCenters = sc.broadcast(centers)
      val bcs = sc.broadcast(s)

      // Find the new centers
      val newCenters = if (!enableMiniBatch) generateNewCenters(data, bcs, bcCenters)
      else generateNewCenters(data.sample(false, sampleRate), bcs, bcCenters)
      converged = true
      newCenters.foreach { case (j, newCenter) =>
        p(j) = KmeansUtil.fastDistance(newCenter, centers(j))
        if (converged && p(j) > epsilon) {
          converged = false
        }
        centers(j) = newCenter
      }
      bcCenters.destroy(blocking = false)
      bcs.destroy(blocking = false)
      iteration += 1
    }
  }
}
