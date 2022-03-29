// scalastyle:off header.matches
/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.mllib.clustering

object KmeansUtil {

  def generateDisMatrix(
      centers: Array[VectorWithNorm], parLevel: Int): Array[Double] = {
    val cl = centers.length
    Array.fill(cl * cl)(0.0)
  }

  def findClosest(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm,
      s: Array[Double]): (Int, Double) = {
    (-1, -1.0)
  }

  def fastDistance(
      v1: VectorWithNorm,
      v2: VectorWithNorm): Double = {
    -1.0
  }

}
