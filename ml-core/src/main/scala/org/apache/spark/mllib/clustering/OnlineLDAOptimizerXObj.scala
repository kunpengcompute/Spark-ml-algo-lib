/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
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

import breeze.linalg.{sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{abs, exp}
import breeze.stats.distributions.Gamma

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}


/**
 * Serializable companion object containing helper methods and shared code for
 * [[OnlineLDAOptimizer]] and [[LocalLDAModel]].
 */
private[clustering] object OnlineLDAOptimizerXObj {
  /**
   * Uses variational inference to infer the topic distribution `gammad` given the term counts
   * for a document. `termCounts` must contain at least one non-zero entry, otherwise Breeze will
   * throw a BLAS error.
   *
   * An optimization (Lee, Seung: Algorithms for non-negative matrix factorization, NIPS 2001)
   * avoids explicit computation of variational parameter `phi`.
   * @see <a href="http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.31.7566">here</a>
   *
   * @return Returns a tuple of `gammad` - estimate of gamma, the topic distribution, `sstatsd` -
   *         statistics for updating lambda and `ids` - list of termCounts vector indices.
   */
  private[clustering] def variationalTopicInference(
      termCounts: Vector,
      expElogbeta: BDM[Double],
      alpha: breeze.linalg.Vector[Double],
      gammaShape: Double,
      k: Int): (BDV[Double], BDM[Double], List[Int]) = {
    val (ids: List[Int], cts: Array[Double]) = termCounts match {
      case v: DenseVector => ((0 until v.size).toList, v.values)
      case v: SparseVector => (v.indices.toList, v.values)
    }
    // Initialize the variational distribution q(theta|gamma) for the mini-batch
    val gammad: BDV[Double] =
      new Gamma(gammaShape, 1.0 / gammaShape).samplesVector(k)                   // K
    val expElogthetad: BDV[Double] = exp(LDAUtilsX.dirichletExpectation(gammad))  // K
    val expElogbetad = expElogbeta(ids, ::).toDenseMatrix                        // ids * K

    val phiNorm: BDV[Double] = expElogbetad * expElogthetad +:+ 1e-100            // ids
    var meanGammaChange = 1D
    val ctsVector = new BDV[Double](cts)                                         // ids

    // Iterate between gamma and phi until convergence
    while (meanGammaChange > 1e-3) {
      val lastgamma = gammad.copy
      //        K                  K * ids               ids
      gammad := (expElogthetad *:* (expElogbetad.t * (ctsVector /:/ phiNorm))) +:+ alpha
      expElogthetad := exp(LDAUtilsX.dirichletExpectation(gammad))
      // TODO: Keep more values in log space, and only exponentiate when needed.
      phiNorm := expElogbetad * expElogthetad +:+ 1e-100
      meanGammaChange = sum(abs(gammad - lastgamma)) / k
    }

    val sstatsd = expElogthetad.asDenseMatrix.t * (ctsVector /:/ phiNorm).asDenseMatrix
    (gammad, sstatsd, ids)
  }
}
