// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.tuning

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession

class RfSurrogate(val ss: SparkSession, val minimize: Boolean = false) {
  def curBest: Double = 0.0

  def update(x: Array[Vector], y: Array[Double]): Unit = {
  }

  def train(): Unit = {
  }

  def predict(x: Array[Vector]): Array[(Double, Double)] = null
}
