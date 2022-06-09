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

import breeze.linalg.{max, sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{exp, log}
import breeze.numerics.DigammaX

import org.apache.spark.ml.tree.impl.TimeTracker


/**
 * Utility methods for LDA.
 */
private[clustering] object LDAUtilsX {
  /**
   * Log Sum Exp with overflow protection using the identity:
   * For any a: $\log \sum_{n=1}^N \exp\{x_n\} = a + \log \sum_{n=1}^N \exp\{x_n - a\}$
   */
  private[clustering] def logSumExp(x: BDV[Double]): Double = {
    val a = max(x)
    a + log(sum(exp(x -:- a)))
  }

  /**
   * For theta ~ Dir(alpha), computes E[log(theta)] given alpha. Currently the implementation
   * uses [[breeze.numerics.digamma]] which is accurate but expensive.
   */
  private[clustering] def dirichletExpectation(alpha: BDV[Double]): BDV[Double] = {
    DigammaX(alpha) - DigammaX(sum(alpha))
  }

  /**
   * Computes [[dirichletExpectation()]] row-wise, assuming each row of alpha are
   * Dirichlet parameters.
   */
  private[clustering] def dirichletExpectation(alpha: BDM[Double],
      timer: TimeTracker = new TimeTracker): BDM[Double] = {
    timer.start("dirichletExpectation-BDM-total")
    timer.start("dirichletExpectation-BDM-sumRowWise")
    val rowSum = sum(alpha(breeze.linalg.*, ::))
    timer.stop("dirichletExpectation-BDM-sumRowWise")
    timer.start("dirichletExpectation-BDM-alphaDigamma")
    val digAlpha = DigammaX(alpha)
    timer.stop("dirichletExpectation-BDM-alphaDigamma")
    timer.start("dirichletExpectation-BDM-rowDigamma")
    val digRowSum = DigammaX(rowSum)
    timer.stop("dirichletExpectation-BDM-rowDigamma")
    timer.start("dirichletExpectation-BDM-result")
    val result = digAlpha(::, breeze.linalg.*) - digRowSum
    timer.stop("dirichletExpectation-BDM-result")
    timer.stop("dirichletExpectation-BDM-total")
    result
  }
}
