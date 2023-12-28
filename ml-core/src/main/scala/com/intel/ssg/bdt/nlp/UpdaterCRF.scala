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
package com.intel.ssg.bdt.nlp

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.mllib.linalg.{Vector => SparkVector}
import org.apache.spark.mllib.optimization.Updater

trait UpdaterCRF extends Updater {
  def compute(
      weightsOld: SparkVector,
      gradient: SparkVector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (SparkVector, Double) = {
    throw new Exception("The original compute() method is not supported")
  }

  def computeCRF(
      weightsOld: BDV[Double],
      gradient: BDV[Double],
      regParam: Double): (BDV[Double], Double)
}
