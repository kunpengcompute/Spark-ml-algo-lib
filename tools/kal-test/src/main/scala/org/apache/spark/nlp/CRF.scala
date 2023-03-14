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
package org.apache.spark.nlp

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.internal.Logging

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

  private  var calcAccuracy: Boolean = false
  def this() = this(freq = 1, regParam = 0.5, maxIterations = 1000, tolerance = 1E-3, regularization = L2)

  def setRegParam(regParam: Double) = {
    this.regParam = regParam
    this
  }

  def setFreq(freq: Int) = {
    this.freq = freq
    this
  }

  def setMaxIterations(maxIterations: Int) = {
    this.maxIterations = maxIterations
    this
  }

  def setEta(eta: Double) = {
    this.tolerance = eta
    this
  }

  def setRegularization(regula: Regularization) = {
    this.regularization = regula
    this
  }

  //set if need to calculate model's accuracy
  //this requires testArrayWithLabel and testArrayWithoutLabel are given
  def setCalcAccuracy(ca: Boolean): this.type = {
    this.calcAccuracy = ca
    this
  }

  /**
   * Internal method to train the CRF model
   *
   * @param template the template to train the model
   * @param trains the source for the training
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
  def runAlgorithm(
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

    featureIdx.alpha = new CRFWithLBFGS(new CRFGradient, updater)
      .setRegParam(regParam)
      .setConvergenceTol(tolerance)
      .setNumIterations(maxIterations)
      .optimizer(taggers, featureIdx.initAlpha())


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
}

/**
  * Top-level methods for calling CRF.
  */
object CRF {

  /**
   * Train CRF Model
   *
   * @param templates Source templates for training the model
   * @param train Source files for training the model
   * @return Model
   */

  def train(
      templates: Array[String],
      train: RDD[Sequence],
      regParam: Double,
      freq: Int,
      maxIteration: Int,
      eta: Double,
      regularization: Regularization): CRFModel = {
    new CRF().setRegParam(regParam)
      .setFreq(freq)
      .setMaxIterations(maxIteration)
      .setEta(eta)
      .setRegularization(regularization)
      .runCRF(templates, train)
  }

  def train(
      templates: Array[String],
      train: RDD[Sequence],
      regParam: Double,
      freq: Int,
      maxIteration: Int,
      eta: Double): CRFModel = {
    new CRF().setRegParam(regParam)
      .setFreq(freq)
      .setMaxIterations(maxIteration)
      .setEta(eta)
      .runCRF(templates, train)
  }

  def train(
      templates: Array[String],
      train: RDD[Sequence],
      regParam: Double,
      freq: Int): CRFModel = {
    new CRF().setRegParam(regParam)
      .setFreq(freq)
      .runCRF(templates, train)
  }

  def train(
      templates: Array[String],
      train: RDD[Sequence],
      regParam: Double,
      regularization: Regularization): CRFModel = {
    new CRF().setRegParam(regParam)
      .setRegularization(regularization)
      .runCRF(templates, train)
  }

  def train(
      templates: Array[String],
      train: RDD[Sequence],
      regularization: Regularization): CRFModel = {
    new CRF().setRegularization(regularization)
      .runCRF(templates, train)
  }

  def train(
      templates: Array[String],
      train: RDD[Sequence]): CRFModel = {
    new CRF().runCRF(templates, train)
  }
}

object Accuracy extends  Logging {
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

    logInfo(f"Prediction Accuracy: $score / $total = ${score / total.toDouble}")

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