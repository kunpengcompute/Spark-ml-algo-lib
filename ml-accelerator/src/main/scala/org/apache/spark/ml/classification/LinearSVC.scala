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

package org.apache.spark.ml.classification

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, OWLQNF}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.StaticUtils
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator._
import org.apache.spark.ml.optim.loss.{L2Regularization, RDDLossFunction}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
 * <a href = "https://en.wikipedia.org/wiki/Support_vector_machine#Linear_SVM">
 *   Linear SVM Classifier</a>
 *
 * This binary classifier optimizes the Hinge Loss using the OWLQN optimizer.
 * Only supports L2 regularization currently.
 *
 * Since 3.1.0, it supports stacking instances into blocks and using GEMV for
 * better performance.
 * The block size will be 1.0 MB, if param maxBlockSizeInMB is set 0.0 by default.
 *
 */
@Since("2.2.0")
class LinearSVC @Since("2.2.0") (
    @Since("2.2.0") override val uid: String)
  extends Classifier[Vector, LinearSVC, LinearSVCModel]
  with LinearSVCParams with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("linearsvc"))

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setRegParam(value: Double): this.type = set(regParam, value)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Whether to fit an intercept term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller values will lead to higher accuracy at the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Whether to standardize the training features before fitting the model.
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setStandardization(value: Boolean): this.type = set(standardization, value)

  /**
   * Set the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Set threshold in binary classification.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setThreshold(value: Double): this.type = set(threshold, value)

  /**
   * Suggested depth for treeAggregate (greater than or equal to 2).
   * If the dimensions of features or the number of partitions are large,
   * this param could be adjusted to a larger size.
   * Default is 2.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  /**
   * Sets the value of param [[maxBlockSizeInMB]].
   * Default is 0.0, then 1.0 MB will be chosen.
   *
   * @group expertSetParam
   */
  @Since("3.1.0")
  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

  @Since("2.2.0")
  override def copy(extra: ParamMap): LinearSVC = defaultCopy(extra)

  override protected def train(dataset: Dataset[_]): LinearSVCModel = instrumented { instr =>
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, weightCol, featuresCol, predictionCol, rawPredictionCol,
      regParam, maxIter, fitIntercept, tol, standardization, threshold, aggregationDepth,
      maxBlockSizeInMB)

    if (dataset.storageLevel != StorageLevel.NONE) {
      instr.logWarning(s"Input instances will be standardized, blockified to blocks, and " +
        s"then cached during training. Be careful of double caching!")
    }

    val instances = extractInstances(dataset)
      .setName("training instances")

    val (summarizer, labelSummarizer) = Summarizer
      .getClassificationSummarizers(instances, $(aggregationDepth), Seq("mean", "std", "count"))

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numFeatures = summarizer.mean.size

    instr.logNumExamples(summarizer.count)
    instr.logNamedValue("lowestLabelWeight", labelSummarizer.histogram.min.toString)
    instr.logNamedValue("highestLabelWeight", labelSummarizer.histogram.max.toString)
    instr.logSumOfWeights(summarizer.weightSum)

    var actualBlockSizeInMB = $(maxBlockSizeInMB)
    if (actualBlockSizeInMB == 0) {
      actualBlockSizeInMB = InstanceBlock.DefaultBlockSizeInMB
      require(actualBlockSizeInMB > 0, "inferred actual BlockSizeInMB must > 0")
      instr.logNamedValue("actualBlockSizeInMB", actualBlockSizeInMB.toString)
    }

    val numClasses = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) =>
        require(n >= histogram.length, s"Specified number of classes $n was " +
          s"less than the number of unique labels ${histogram.length}.")
        n
      case None => histogram.length
    }
    require(numClasses == 2, s"LinearSVC only supports binary classification." +
      s" $numClasses classes detected in $labelCol")
    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)

    if (numInvalid != 0) {
      val msg = s"Classification labels should be in [0 to ${numClasses - 1}]. " +
        s"Found $numInvalid invalid labels."
      instr.logError(msg)
      throw new SparkException(msg)
    }

    val featuresStd = summarizer.std.toArray
    val featuresMean = summarizer.mean.toArray
    val getFeaturesStd = (j: Int) => featuresStd(j)
    val regularization = if ($(regParam) != 0.0) {
      val shouldApply = (idx: Int) => idx >= 0 && idx < numFeatures
      Some(new L2Regularization($(regParam), shouldApply,
        if ($(standardization)) None else Some(getFeaturesStd)))
    } else None

    def regParamL1Fun = (index: Int) => 0.0
    val optimizer = new OWLQNF($(maxIter), 10, regParamL1Fun, $(tol))

    /*
       The coefficients are trained in the scaled space; we're converting them back to
       the original space.
       Note that the intercept in scaled space and original space is the same;
       as a result, no scaling is needed.
     */
    val (rawCoefficients, objectiveHistory) =
      trainImpl(instances, actualBlockSizeInMB, featuresStd, featuresMean,
        regularization, optimizer)

    if (rawCoefficients == null) {
      val msg = s"${optimizer.getClass.getName} failed."
      instr.logError(msg)
      throw new SparkException(msg)
    }

    val coefficientArray = Array.tabulate(numFeatures) { i =>
      if (featuresStd(i) != 0.0) rawCoefficients(i) / featuresStd(i) else 0.0
    }
    val intercept = if ($(fitIntercept)) rawCoefficients.last else 0.0
    createModel(dataset, Vectors.dense(coefficientArray), intercept, objectiveHistory)
  }

  private def createModel(
      dataset: Dataset[_],
      coefficients: Vector,
      intercept: Double,
      objectiveHistory: Array[Double]): LinearSVCModel = {
    val model = copyValues(new LinearSVCModel(uid, coefficients, intercept))
    val weightColName = if (!isDefined(weightCol)) "weightCol" else $(weightCol)

    val (summaryModel, rawPredictionColName, predictionColName) = model.findSummaryModel()
    val summary = new LinearSVCTrainingSummaryImpl(
      summaryModel.transform(dataset),
      rawPredictionColName,
      predictionColName,
      $(labelCol),
      weightColName,
      objectiveHistory)
    model.setSummary(Some(summary))
  }

  private def trainImpl(
      instances: RDD[Instance],
      actualBlockSizeInMB: Double,
      featuresStd: Array[Double],
      featuresMean: Array[Double],
      regularization: Option[L2Regularization],
      optimizer: OWLQNF): (Array[Double], Array[Double]) = {
    val numFeatures = featuresStd.length
    val numFeaturesPlusIntercept = if ($(fitIntercept)) numFeatures + 1 else numFeatures

    val inverseStd = featuresStd.map(std => if (std != 0) 1.0 / std else 0.0)
    val scaledMean = Array.tabulate(numFeatures)(i => inverseStd(i) * featuresMean(i))
    val bcInverseStd = instances.context.broadcast(inverseStd)
    val bcScaledMean = instances.context.broadcast(scaledMean)

    val standardized = instances.mapPartitions { iter =>
      val func = StandardScalerModel.getTransformFunc(Array.empty, bcInverseStd.value, false, true)
      iter.map { case Instance(label, weight, vec) =>
        Instance(label, weight + StaticUtils.ZERO_DOUBLE, func(vec)) }
    }

    val maxMemUsage = (actualBlockSizeInMB * 1024L * 1024L).ceil.toLong
    val blocks = InstanceBlock.blokifyWithMaxMemUsage(standardized, maxMemUsage)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .setName(s"training blocks (blockSizeInMB=$actualBlockSizeInMB)")

    val getAggregatorFunc = new HingeBlockAggregator(bcInverseStd, bcScaledMean,
      $(fitIntercept))(_)
    val costFun = new RDDLossFunction(blocks, getAggregatorFunc,
      regularization, $(aggregationDepth))

    val initialSolution = Array.ofDim[Double](numFeaturesPlusIntercept)
    if ($(fitIntercept)) {
      // orginal `initialSolution` is for problem:
      // y = f(w1 * x1 / std_x1, w2 * x2 / std_x2, ..., intercept)
      // we should adjust it to the initial solution for problem:
      // y = f(w1 * (x1 - avg_x1) / std_x1, w2 * (x2 - avg_x2) / std_x2, ..., intercept)
      // NOTE: this is NOOP before we finally support model initialization
      val adapt = BLAS.javaBLAS.ddot(numFeatures, initialSolution, 1, scaledMean, 1)
      initialSolution(numFeatures) += adapt
    }

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      new BDV[Double](initialSolution))
    val arrayBuilder = mutable.ArrayBuilder.make[Double]
    var state: optimizer.State = null
    while (states.hasNext) {
      state = states.next()
      arrayBuilder += state.adjustedValue
    }
    blocks.unpersist()
    bcInverseStd.destroy()
    bcScaledMean.destroy()

    val solution = if (state == null) null else state.x.toArray
    if ($(fitIntercept) && solution != null) {
      // the final solution is for problem:
      // y = f(w1 * (x1 - avg_x1) / std_x1, w2 * (x2 - avg_x2) / std_x2, ..., intercept)
      // we should adjust it back for original problem:
      // y = f(w1 * x1 / std_x1, w2 * x2 / std_x2, ..., intercept)
      val adapt = BLAS.javaBLAS.ddot(numFeatures, solution, 1, scaledMean, 1)
      solution(numFeatures) -= adapt
    }
    (solution, arrayBuilder.result)
  }
}

@Since("2.2.0")
object LinearSVC extends DefaultParamsReadable[LinearSVC] {

  @Since("2.2.0")
  override def load(path: String): LinearSVC = super.load(path)
}
