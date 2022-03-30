// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.mllib.feature

import org.apache.spark.internal.Logging
import org.apache.spark.ml.StaticUtils
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD

class SPCA(val k: Int) extends Logging {

  def setMode(mode: String): this.type = null

  def setHighDimensionThreshold(highDimensionThreshold: Int): this.type = null

  def setNThread(nThread: Int): this.type = null

  def fit(sources: RDD[Vector]): PCAModel = null

  def evaluate(
      rows: RDD[SparseVector],
      nRows: Long,
      nCols: Int,
      nThread: Int,
      explainedVariance: Array[Double],
      highDimensionThreshold: Int = 10000000): Double = 0.0
}
