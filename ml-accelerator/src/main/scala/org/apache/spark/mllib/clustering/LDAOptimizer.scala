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

import java.util.Random

import breeze.linalg.{all, sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{abs, exp, trigamma}
import breeze.stats.distributions.{Gamma, RandBasis}

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.tree.impl.TimeTracker
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 *
 * An LDAOptimizer specifies which optimization/learning/inference algorithm to use, and it can
 * hold optimizer-specific parameters for users to set.
 */
@Since("1.4.0")
@DeveloperApi
trait LDAOptimizer {

  /*
    DEVELOPERS NOTE:

    An LDAOptimizer contains an algorithm for LDA and performs the actual computation, which
    stores internal data structure (Graph or Matrix) and other parameters for the algorithm.
    The interface is isolated to improve the extensibility of LDA.
   */

  /**
   * Initializer for the optimizer. LDA passes the common parameters to the optimizer and
   * the internal structure can be initialized properly.
   */
  private[clustering] def initialize(docs: RDD[(Long, Vector)], lda: LDA): LDAOptimizer

  private[clustering] def next(): LDAOptimizer = new OnlineLDAOptimizer

  private[clustering] def next(timer: TimeTracker): LDAOptimizer = new OnlineLDAOptimizer

  private[clustering] def getLDAModel(iterationTimes: Array[Double]): LDAModel
}

/**
 * :: DeveloperApi ::
 *
 * An online optimizer for LDA. The Optimizer implements the Online variational Bayes LDA
 * algorithm, which processes a subset of the corpus on each iteration, and updates the term-topic
 * distribution adaptively.
 *
 * Original Online LDA paper:
 *   Hoffman, Blei and Bach, "Online Learning for Latent Dirichlet Allocation." NIPS, 2010.
 */
@Since("1.4.0")
@DeveloperApi
final class OnlineLDAOptimizer extends LDAOptimizer with Logging {

  // LDA common parameters
  private var k: Int = 0
  private var corpusSize: Long = 0
  private var vocabSize: Int = 0

  /** alias for docConcentration */
  private var alpha: Vector = Vectors.dense(0)

  /** (for debugging)  Get docConcentration */
  private[clustering] def getAlpha: Vector = alpha

  /** alias for topicConcentration */
  private var eta: Double = 0

  /** (for debugging)  Get topicConcentration */
  private[clustering] def getEta: Double = eta

  private var randomGenerator: java.util.Random = null

  /** (for debugging) Whether to sample mini-batches with replacement. (default = true) */
  private var sampleWithReplacement: Boolean = true

  // Online LDA specific parameters
  // Learning rate is: (tau0 + t)^{-kappa}
  private var tau0: Double = 1024
  private var kappa: Double = 0.51
  private var miniBatchFraction: Double = 0.05
  private var optimizeDocConcentration: Boolean = false

  // internal data structure
  private var docs: RDD[(Long, Vector)] = null

  /** Dirichlet parameter for the posterior over topics */
  private var lambda: BDM[Double] = null

  /** (for debugging) Get parameter for topics */
  private[clustering] def getLambda: BDM[Double] = lambda

  /** Current iteration (count of invocations of [[next()]]) */
  private var iteration: Int = 0
  private var gammaShape: Double = 100

  /**
   * A (positive) learning parameter that downweights early iterations. Larger values make early
   * iterations count less.
   */
  @Since("1.4.0")
  def getTau0: Double = this.tau0

  /**
   * A (positive) learning parameter that downweights early iterations. Larger values make early
   * iterations count less.
   * Default: 1024, following the original Online LDA paper.
   */
  @Since("1.4.0")
  def setTau0(tau0: Double): this.type = {
    require(tau0 > 0, s"LDA tau0 must be positive, but was set to $tau0")
    this.tau0 = tau0
    this
  }

  /**
   * Learning rate: exponential decay rate
   */
  @Since("1.4.0")
  def getKappa: Double = this.kappa

  /**
   * Learning rate: exponential decay rate---should be between
   * (0.5, 1.0] to guarantee asymptotic convergence.
   * Default: 0.51, based on the original Online LDA paper.
   */
  @Since("1.4.0")
  def setKappa(kappa: Double): this.type = {
    require(kappa >= 0, s"Online LDA kappa must be nonnegative, but was set to $kappa")
    this.kappa = kappa
    this
  }

  /**
   * Mini-batch fraction, which sets the fraction of document sampled and used in each iteration
   */
  @Since("1.4.0")
  def getMiniBatchFraction: Double = this.miniBatchFraction

  /**
   * Mini-batch fraction in (0, 1], which sets the fraction of document sampled and used in
   * each iteration.
   *
   * @note This should be adjusted in synch with `LDA.setMaxIterations()`
   * so the entire corpus is used.  Specifically, set both so that
   * maxIterations * miniBatchFraction is at least 1.
   *
   * Default: 0.05, i.e., 5% of total documents.
   */
  @Since("1.4.0")
  def setMiniBatchFraction(miniBatchFraction: Double): this.type = {
    require(miniBatchFraction > 0.0 && miniBatchFraction <= 1.0,
      s"Online LDA miniBatchFraction must be in range (0,1], but was set to $miniBatchFraction")
    this.miniBatchFraction = miniBatchFraction
    this
  }

  /**
   * Optimize docConcentration, indicates whether docConcentration (Dirichlet parameter for
   * document-topic distribution) will be optimized during training.
   */
  @Since("1.5.0")
  def getOptimizeDocConcentration: Boolean = this.optimizeDocConcentration

  /**
   * Sets whether to optimize docConcentration parameter during training.
   *
   * Default: false
   */
  @Since("1.5.0")
  def setOptimizeDocConcentration(optimizeDocConcentration: Boolean): this.type = {
    this.optimizeDocConcentration = optimizeDocConcentration
    this
  }

  /**
   * Set the Dirichlet parameter for the posterior over topics.
   * This is only used for testing now. In the future, it can help support training stop/resume.
   */
  private[clustering] def setLambda(lambda: BDM[Double]): this.type = {
    this.lambda = lambda
    this
  }

  /**
   * Used for random initialization of the variational parameters.
   * Larger value produces values closer to 1.0.
   * This is only used for testing currently.
   */
  private[clustering] def setGammaShape(shape: Double): this.type = {
    this.gammaShape = shape
    this
  }

  /**
   * Sets whether to sample mini-batches with or without replacement. (default = true)
   * This is only used for testing currently.
   */
  private[clustering] def setSampleWithReplacement(replace: Boolean): this.type = {
    this.sampleWithReplacement = replace
    this
  }

  override private[clustering] def initialize(
      docs: RDD[(Long, Vector)],
      lda: LDA): OnlineLDAOptimizer = {
    this.k = lda.getK
    this.corpusSize = docs.count()
    this.vocabSize = docs.first()._2.size
    this.alpha = if (lda.getAsymmetricDocConcentration.size == 1) {
      if (lda.getAsymmetricDocConcentration(0) == -1) Vectors.dense(Array.fill(k)(1.0 / k))
      else {
        require(lda.getAsymmetricDocConcentration(0) >= 0,
          s"all entries in alpha must be >=0, got: $alpha")
        Vectors.dense(Array.fill(k)(lda.getAsymmetricDocConcentration(0)))
      }
    } else {
      require(lda.getAsymmetricDocConcentration.size == k,
        s"alpha must have length k, got: $alpha")
      lda.getAsymmetricDocConcentration.foreachActive { case (_, x) =>
        require(x >= 0, s"all entries in alpha must be >= 0, got: $alpha")
      }
      lda.getAsymmetricDocConcentration
    }
    this.eta = if (lda.getTopicConcentration == -1) 1.0 / k else lda.getTopicConcentration
    this.randomGenerator = new Random(lda.getSeed)

    this.docs = docs

    // Initialize the variational distribution q(beta|lambda)
    this.lambda = getGammaMatrix(k, vocabSize)
    this.iteration = 0
    this
  }

  override private[clustering] def next(timer: TimeTracker = new TimeTracker)
      : OnlineLDAOptimizer = {
    timer.start("sample&judge")
    val batch = docs.sample(withReplacement = sampleWithReplacement, miniBatchFraction,
      randomGenerator.nextLong())
    timer.stop("sample&judge")
    if (batch.isEmpty()) return this

    timer.start("submitMiniBatch")
    val x = submitMiniBatch(batch, timer)
    timer.stop("submitMiniBatch")
    x
  }

  /**
   * Submit a subset (like 1%, decide by the miniBatchFraction) of the corpus to the Online LDA
   * model, and it will update the topic distribution adaptively for the terms appearing in the
   * subset.
   */
  private[clustering] def submitMiniBatch(batch: RDD[(Long, Vector)],
      timer: TimeTracker = new TimeTracker): OnlineLDAOptimizer = {
    iteration += 1
    val k = this.k
    val vocabSize = this.vocabSize

    timer.start("dirichletExpectation-lambda")
    val expElogbeta = LDAUtilsXOpt.dirichletExpectationFastExp(lambda, timer).t
    timer.stop("dirichletExpectation-lambda")

    timer.start("broadcast-expElogbeta")
    val expElogbetaBc = batch.sparkContext.broadcast(expElogbeta)
    timer.stop("broadcast-expElogbeta")
    val alpha = this.alpha.asBreeze
    val gammaShape = this.gammaShape
    val optimizeDocConcentration = this.optimizeDocConcentration
    // If and only if optimizeDocConcentration is set true,
    // we calculate logphat in the same pass as other statistics.
    // No calculation of loghat happens otherwise.
    val logphatPartOptionBase = () => if (optimizeDocConcentration) {
                                        Some(BDV.zeros[Double](k))
                                      } else {
                                        None
                                      }
    timer.start("docs-mapPartitions")

    val stats: RDD[(BDM[Double], Option[BDV[Double]], Long)] =
      if (LDAUtilsXOpt.useOptimizedCalc()) {
        LDAUtilsXOpt.optimizedCalcStats(batch, expElogbetaBc, k, vocabSize, logphatPartOptionBase,
          alpha, gammaShape)
    } else {
      batch.mapPartitions { docs =>
        val nonEmptyDocs = docs.filter(_._2.numNonzeros > 0)

        val stat = BDM.zeros[Double](k, vocabSize)
        val logphatPartOption = logphatPartOptionBase()
        var nonEmptyDocCount: Long = 0L
        val expElogbetaBcValue = expElogbetaBc.value
        nonEmptyDocs.foreach { case (_, termCounts: Vector) =>
          nonEmptyDocCount += 1
          val (gammad, sstats, ids) = OnlineLDAOptimizerXObj.variationalTopicInference(
            termCounts, expElogbetaBcValue, alpha, gammaShape, k)
          stat(::, ids) := stat(::, ids) + sstats
          logphatPartOption.foreach(_ += LDAUtilsX.dirichletExpectation(gammad))
        }
        Iterator((stat, logphatPartOption, nonEmptyDocCount))
      }
    }

    timer.stop("docs-mapPartitions")

    val elementWiseSum = (
        u: (BDM[Double], Option[BDV[Double]], Long),
        v: (BDM[Double], Option[BDV[Double]], Long)) => {
      u._1 += v._1
      u._2.foreach(_ += v._2.get)
      (u._1, u._2, u._3 + v._3)
    }

    timer.start("stats-treeAggregate")
    val (statsSum: BDM[Double], logphatOption: Option[BDV[Double]], nonEmptyDocsN: Long) =
      if (LDAUtilsXOpt.useOptimizedAgg(batch)) {
        LDAUtilsXOpt.optimizedAggregateStats(stats)
      } else {
        stats
          .treeAggregate((BDM.zeros[Double](k, vocabSize), logphatPartOptionBase(), 0L))(
            elementWiseSum, elementWiseSum
          )
      }

    timer.stop("stats-treeAggregate")

    timer.start("update-lambda")
    expElogbetaBc.destroy(false)

    if (nonEmptyDocsN == 0) {
      logWarning("No non-empty documents were submitted in the batch.")
      // Therefore, there is no need to update any of the model parameters
      return this
    }

    val batchResult = statsSum *:* expElogbeta.t
    // Note that this is an optimization to avoid batch.count
    val batchSize = (miniBatchFraction * corpusSize).ceil.toInt
    updateLambdaMT(batchResult, batchSize)
    timer.stop("update-lambda")

    logphatOption.foreach(_ /= nonEmptyDocsN.toDouble)
    logphatOption.foreach(updateAlpha(_, nonEmptyDocsN))

    this
  }

  /**
   * Update lambda based on the batch submitted. batchSize can be different for each iteration.
   */
  private def updateLambda(stat: BDM[Double], batchSize: Int): Unit = {
    // weight of the mini-batch.
    val weight = rho()

    // Update lambda based on documents.
    lambda := (1 - weight) * lambda +
      weight * (stat * (corpusSize.toDouble / batchSize.toDouble) + eta)
  }

  private def updateLambdaMT(stat: BDM[Double], batchSize: Int): Unit = {
    if (LDAUtilsXOpt.useOptimizedUpdateLambda()) {
      val weight = rho()
      LDAUtilsXOpt.optimizedUpdateLambda(stat, lambda, weight, corpusSize, batchSize, eta)
    } else {
      updateLambda(stat, batchSize)
    }
  }

  /**
   * Update alpha based on `logphat`.
   * Uses Newton-Rhapson method.
   * @see Section 3.3, Huang: Maximum Likelihood Estimation of Dirichlet Distribution Parameters
   *      (http://jonathan-huang.org/research/dirichlet/dirichlet.pdf)
   * @param logphat Expectation of estimated log-posterior distribution of
   *                topics in a document averaged over the batch.
   * @param nonEmptyDocsN number of non-empty documents
   */
  private def updateAlpha(logphat: BDV[Double], nonEmptyDocsN: Double): Unit = {
    val weight = rho()
    val alpha = this.alpha.asBreeze.toDenseVector

    val gradf = nonEmptyDocsN * (-LDAUtilsX.dirichletExpectation(alpha) + logphat)

    val c = nonEmptyDocsN * trigamma(sum(alpha))
    val q = -nonEmptyDocsN * trigamma(alpha)
    val b = sum(gradf / q) / (1D / c + sum(1D / q))

    val dalpha = -(gradf - b) / q

    if (all((weight * dalpha + alpha) >:> 0D)) {
      alpha :+= weight * dalpha
      this.alpha = Vectors.dense(alpha.toArray)
    }
  }


  /** Calculate learning rate rho for the current [[iteration]]. */
  private def rho(): Double = {
    math.pow(getTau0 + this.iteration, -getKappa)
  }

  /**
   * Get a random matrix to initialize lambda.
   */
  private def getGammaMatrix(row: Int, col: Int): BDM[Double] = {
    val randBasis = new RandBasis(new org.apache.commons.math3.random.MersenneTwister(
      randomGenerator.nextLong()))
    val gammaRandomGenerator = new Gamma(gammaShape, 1.0 / gammaShape)(randBasis)
    val temp = gammaRandomGenerator.sample(row * col).toArray
    new BDM[Double](col, row, temp).t
  }

  override private[clustering] def getLDAModel(iterationTimes: Array[Double]): LDAModel = {
    new LocalLDAModel(Matrices.fromBreeze(lambda).transpose, alpha, eta, gammaShape)
  }

}

/**
 * Serializable companion object containing helper methods and shared code for
 * [[OnlineLDAOptimizer]] and [[LocalLDAModel]].
 */
private[clustering] object OnlineLDAOptimizer {
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
    val expElogthetad: BDV[Double] = exp(LDAUtils.dirichletExpectation(gammad))  // K
    val expElogbetad = expElogbeta(ids, ::).toDenseMatrix                        // ids * K

    val phiNorm: BDV[Double] = expElogbetad * expElogthetad +:+ 1e-100            // ids
    var meanGammaChange = 1D
    val ctsVector = new BDV[Double](cts)                                         // ids

    // Iterate between gamma and phi until convergence
    while (meanGammaChange > 1e-3) {
      val lastgamma = gammad.copy
      //        K                  K * ids               ids
      gammad := (expElogthetad *:* (expElogbetad.t * (ctsVector /:/ phiNorm))) +:+ alpha
      expElogthetad := exp(LDAUtils.dirichletExpectation(gammad))
      // TODO: Keep more values in log space, and only exponentiate when needed.
      phiNorm := expElogbetad * expElogthetad +:+ 1e-100
      meanGammaChange = sum(abs(gammad - lastgamma)) / k
    }

    val sstatsd = expElogthetad.asDenseMatrix.t * (ctsVector /:/ phiNorm).asDenseMatrix
    (gammad, sstatsd, ids)
  }
}
