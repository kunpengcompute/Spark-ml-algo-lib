// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.tuning

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession

class Solver(
    val ss: SparkSession,
    val paramSpace: ParamSpace,
    val minimize: Boolean,
    val batchSize: Int,
    val sampleSize: Int) {
  def getHistory: (Array[Vector], Array[Double]) = null

  def suggest(): Array[ParamMap] = null

  def feed(configs: Array[ParamMap], y: Array[Double]): Unit = { }

  def feed(config: ParamMap, y: Double): Unit = { }
}
