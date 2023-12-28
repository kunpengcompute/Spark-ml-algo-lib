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
// scalastyle:off
package com.intel.ssg.bdt.nlp

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

trait Regularization

case object L1 extends Regularization

case object L2 extends Regularization

/**
 * CRF with support for multiple parallel runs
 * L2 regParam = 1/(2.0 * sigma**2)
 */
class CRF private (
    private var freq: Int,
    private var regParam: Double,
    private var maxIterations: Int,
    private var tolerance: Double,
    private var regularization: Regularization) extends Serializable with Logging {
  // boost-kit parameter
  private var compLevel: Int = 0
  private var nThread: Int = 1
  private var globalStageIterFraction: Double = 1.0
  private var commFreeSplit: Int = 0
  private var commFreeToleranceFactor: Double = 5.0
  private var calcAccuracy: Boolean = false

  def this() = this(
    freq = 1,
    regParam = 0.5,
    maxIterations = 1000,
    tolerance = 1E-3,
    regularization = L2)

  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  // set features that frequency greater than given value
  def setFreq(freq: Int): this.type = {
    this.freq = freq
    this
  }

  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  def setTolerance(tol: Double): this.type = {
    this.tolerance = tol
    this
  }

  def setRegularization(reg: Regularization): this.type = {
    this.regularization = reg
    this
  }

  // set if need to calculate model's accuracy
  // this requires testArrayWithLabel and testArrayWithoutLabel are given
  def setCalcAccuracy(ca: Boolean): this.type = {
    this.calcAccuracy = ca
    this
  }

  // set data compression level
  def setCompLevel(compLevel: Int): this.type = {
    require(compLevel >= 0 && compLevel <= 3,
      s"compLevel must be [0, 1, 2, 3] but got $compLevel")
    this.compLevel = compLevel
    this
  }

  // set global Stage Iteration Fraction
  def setGlobalStageIterFraction(globalStageIterFraction: Double): this.type = {
    require(globalStageIterFraction >= 0.0 && globalStageIterFraction <= 1.0,
      s"globalStageIterFraction must be [0.0, 1.0] but got $globalStageIterFraction")
    this.globalStageIterFraction = globalStageIterFraction
    this
  }

  // set number of partitions for local stage
  def setCommFreeSplit(commFreeSplit: Int): this.type = {
    require(commFreeSplit >= 0,
      s"commFreeSplit must be greater or equal than 0, but got $commFreeSplit")
    this.commFreeSplit = commFreeSplit
    this
  }

  // set Tolerance Factor for local stage
  def setCommFreeToleranceFactor(commFreeToleranceFactor: Double): this.type = {
    require(commFreeToleranceFactor >= 1.0 && commFreeToleranceFactor <= 10.0,
      s"commFreeToleranceFactor must be in range of [1.0, 10.0], " +
        s"but got $commFreeToleranceFactor")
    this.commFreeToleranceFactor = commFreeToleranceFactor
    this
  }

  // set number of thread
  def setNumThread(numThread: Int): this.type = {
    require(numThread >= 1, s"numThread must be greater than 0 but got $numThread")
    this.nThread = numThread
    this
  }

  /**
   * train CRF model
   *
   * @param template the template to train the model
   * @param trains the source for the training
   * @param testArrayWithLabel test dataset with label
   * @param testArrayWithoutLabel test dataset without label
   * @return the model of the source
   */
  def runCRF(
    template: Array[String],
    trains: RDD[Sequence],
    testArrayWithLabel: Array[Sequence] = Array[Sequence](),
    testArrayWithoutLabel: Array[Sequence] = Array[Sequence]()): CRFModel = {
    val featureIdx = new FeatureIndex()
    featureIdx.openTemplate(template)
    featureIdx.openTagSetDist(trains)

    val bcFeatureIdxI: Broadcast[FeatureIndex] = trains.context.broadcast(featureIdx)
    val taggers = trains.map(train => {
      val tagger: Tagger = new Tagger(bcFeatureIdxI.value.labels.size, LearnMode)
      tagger.read(train, bcFeatureIdxI.value)
      tagger
    })

    featureIdx.buildDictionaryDist(taggers, bcFeatureIdxI, freq)

    val bcFeatureIdxII = trains.context.broadcast(featureIdx)
    val taggerList: RDD[Tagger] = taggers.map(bcFeatureIdxII.value.buildFeatures).cache()

    val model = runAlgorithm(taggerList, featureIdx, testArrayWithLabel, testArrayWithoutLabel)
    taggerList.unpersist()

    model
  }

  /**
   *
   * @param taggers the tagger in the template
   * @param featureIdx the index of the feature
   */
  private def runAlgorithm(
      taggers: RDD[Tagger],
      featureIdx: FeatureIndex,
      testArrayWithLabel: Array[Sequence] = Array[Sequence](),
      testArrayWithoutLabel: Array[Sequence] = Array[Sequence]()): CRFModel = {

    logInfo("Starting CRF Iterations ( sentences: %d, features: %d, labels: %d )"
      .format(taggers.count(), featureIdx.maxID, featureIdx.labels.length))

    var updater: UpdaterCRF = null
    regularization match {
      case L1 =>
        updater = new L1Updater
      case L2 =>
        updater = new L2Updater
      case _ =>
        throw new Exception("only support L1-CRF and L2-CRF now")
    }

    if (compLevel == 0 && nThread == 1) {
      featureIdx.alpha = new CRFWithLBFGS(new CRFGradient, updater)
        .setRegParam(regParam)
        .setConvergenceTol(tolerance)
        .setNumIterations(maxIterations)
        .optimizer(taggers, featureIdx.initAlpha())
    } else if (globalStageIterFraction != 1.0) {
      val CRFObj = new CRFWithLBFGS(new CRFGradient, updater, compLevel, nThread)
        .setRegParam(regParam)
        .setConvergenceTol(tolerance * commFreeToleranceFactor)
        .setNumIterations((maxIterations * (1.0 - globalStageIterFraction)).toInt)

      featureIdx.alpha = runTwoStageCRF(CRFObj, taggers, featureIdx)
    } else {
      featureIdx.alpha = new CRFWithLBFGS(new CRFGradient, updater, compLevel, nThread)
        .setRegParam(regParam)
        .setConvergenceTol(tolerance)
        .setNumIterations(maxIterations)
        .optimizerX(taggers, featureIdx.initAlpha())
    }

    // calculate the accuracy faster
    if (calcAccuracy && testArrayWithLabel.length == testArrayWithoutLabel.length) {
      if (testArrayWithLabel.length != 0) {
        Accuracy.calc(featureIdx, testArrayWithLabel, testArrayWithoutLabel)
      } else {
        logInfo(s"test dataset not given.")
      }
    }

    featureIdx.saveModel
  }

  private def runTwoStageCRF(
      CRFWithLBFGSObj: CRFWithLBFGS,
      taggers: RDD[Tagger],
      featureIdx: FeatureIndex): BDV[Double] = {
    val numParts = taggers.getNumPartitions
    val taggersStage1 = if (commFreeSplit != 0) {
      taggers.repartition(commFreeSplit).cache()
    } else {
      taggers.cache()
    }

    // stage1: local training
    val weightsIdsRDD: RDD[BDV[Double]] = taggersStage1.mapPartitions { tgr =>
      Iterator(CRFWithLBFGSObj.optimizerLocal(tgr, featureIdx.initAlpha()))
    }

    // sum up local weights and then average
    val weightsAveraged = weightsIdsRDD.reduce((weight1, weight2) => weight1 +:+ weight2)
    featureIdx.alpha = weightsAveraged *:* (1 / taggersStage1.getNumPartitions.toDouble)

    taggersStage1.unpersist()

    // reset the tolerance and number of iterations for global stage
    val globalIter = (maxIterations * globalStageIterFraction).toInt
    CRFWithLBFGSObj.setConvergenceTol(tolerance).setNumIterations(globalIter)

    val taggersStage2 = taggers.repartition(numParts).cache()

    // stage2: global training
    featureIdx.alpha = CRFWithLBFGSObj.optimizerX(taggersStage2, featureIdx.alpha)
    taggersStage2.unpersist()

    featureIdx.alpha
  }
}


object Accuracy extends Logging {
  def calc(
      featureIdx: FeatureIndex,
      testArrayWithLabel: Array[Sequence],
      testArrayWithoutLabel: Array[Sequence]): Double = {
    val results = testArrayWithoutLabel.map(testCRF(_, featureIdx))
    var score = 0
    var i = 0
    for (r <- results) {
      score += r.compare(testArrayWithLabel(i))
      i += 1
    }
    val total = testArrayWithoutLabel.map(_.toArray.length).sum

    logInfo(f"==== Prediction Accuracy: $score / $total = ${score / total.toDouble} ====")

    score / total.toDouble
  }

  private def testCRF(test: Sequence, featureIdx: FeatureIndex): Sequence = {
    val tagger = new Tagger(featureIdx.labels.size, TestMode)
    tagger.read(test, featureIdx)
    featureIdx.buildFeatures(tagger)
    tagger.parse(featureIdx.alpha, None)

    Sequence(test.toArray.map { x =>
      Token.put(featureIdx.labels(tagger.result(test.toArray.indexOf(x))), x.tags)
    })
  }
}
