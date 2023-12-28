// scalastyle:off
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.nlp

import breeze.optimize.DiffFunction
import com.intel.ssg.bdt.nlp.Tagger
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

class CRFGradientX extends Serializable {
}

object CRFGradientX {
  def dataProcess(data: RDD[Tagger], nThread: Int): RDD[Array[ArrayBuffer[Tagger]]] = {
    null
  }
}

class CostFunX(
    Data: RDD[Array[ArrayBuffer[Tagger]]],
    gradient: CRFGradientX,
    updater: Updater,
    regParam: Double,
    compLevel: Int,
    numThread: Int) extends DiffFunction[BDV[Double]] with Logging with Serializable {

  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
    null
  }

  def setDriverCoreFromSparkConf(sc: SparkContext): Unit = {
  }
}
