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

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Vector => BV}

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.tree.impl.TimeTracker
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object LDAUtilsXOpt {
  def init(sparkContext: SparkContext): Unit = {
  }

  def dirichletExpectationFast(alpha: BDM[Double],
      timer: TimeTracker = new TimeTracker): BDM[Double] = {
    null
  }

  def dirichletExpectationFastExp(alpha: BDM[Double],
      timer: TimeTracker = new TimeTracker): BDM[Double] = {
    null
  }

  def calcIndex(rows: Int, maxThread: Int): (Array[(Int, Int)], Int) = {
    null
  }

  def useOptimizedCalc(): Boolean = {
    true
  }

  def optimizedCalcStats(batch: RDD[(Long, Vector)],
      expElogbetaBc: Broadcast[BDM[Double]],
      k: Int,
      vocabSize: Int,
      logphatPartOptionBase: () => Option[BDV[Double]],
      alpha: BV[Double],
      gammaShape: Double,
      seed: Long): RDD[(BDM[Double], Option[BDV[Double]], Long)] = {
    null
  }

  def useOptimizedAgg(stats: RDD[(Long, Vector)]): Boolean = {
    true
  }

  def optimizedAggregateStats(stats: RDD[(BDM[Double], Option[BDV[Double]], Long)]) :
  (BDM[Double], Option[BDV[Double]], Long) = {
    null
  }

  def useOptimizedUpdateLambda() : Boolean = {
    true
  }

  def optimizedUpdateLambda(stat: BDM[Double],
      lambda: BDM[Double],
      weight: Double,
      corpusSize: Long,
      batchSize: Int,
      eta: Double) : Unit = {
  }
}
