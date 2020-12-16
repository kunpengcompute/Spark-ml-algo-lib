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

import java.util.Locale

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, LBFGSL, OWLQNL}

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator.LogisticAggregatorX
import org.apache.spark.ml.optim.loss.{L2Regularization, RDDLossFunctionX}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel

/**
 * Logistic regression. Supports:
 *  - Multinomial logistic (softmax) regression.
 *  - Binomial logistic regression.
 *
 * This class supports fitting traditional logistic regression model by LBFGS/OWLQN and
 * bound (box) constrained logistic regression model by LBFGSB.
 */
@Since("1.2.0")
class LogisticRegression @Since("1.2.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, LogisticRegression, LogisticRegressionModel]
  with LogisticRegressionParams with DefaultParamsWritable with Logging {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("logreg"))

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("1.2.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Set the ElasticNet mixing parameter.
   * For alpha = 0, the penalty is an L2 penalty.
   * For alpha = 1, it is an L1 penalty.
   * For alpha in (0,1), the penalty is a combination of L1 and L2.
   * Default is 0.0 which is an L2 penalty.
   *
   * Note: Fitting under bound constrained optimization only supports L2 regularization,
   * so throws exception if this param is non-zero value.
   *
   * @group setParam
   */
  @Since("1.4.0")
  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)
  setDefault(elasticNetParam -> 0.0)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("1.2.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy at the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("1.4.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Whether to fit an intercept term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("1.4.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /**
   * Sets the value of param [[family]].
   * Default is "auto".
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setFamily(value: String): this.type = set(family, value)
  setDefault(family -> "auto")

  /**
   * Whether to standardize the training features before fitting the model.
   * The coefficients of models will be always returned on the original scale,
   * so it will be transparent for users. Note that with/without standardization,
   * the models should be always converged to the same solution when no regularization
   * is applied. In R's GLMNET package, the default behavior is true as well.
   * Default is true.
   *
   * @group setParam
   */
  @Since("1.5.0")
  def setStandardization(value: Boolean): this.type = set(standardization, value)
  setDefault(standardization -> true)

  @Since("1.5.0")
  override def setThreshold(value: Double): this.type = super.setThreshold(value)
  setDefault(threshold -> 0.5)

  @Since("1.5.0")
  override def getThreshold: Double = super.getThreshold

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("1.6.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  @Since("1.5.0")
  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  @Since("1.5.0")
  override def getThresholds: Array[Double] = super.getThresholds

  /**
   * Suggested depth for treeAggregate (greater than or equal to 2).
   * If the dimensions of features or the number of partitions are large,
   * this param could be adjusted to a larger size.
   * Default is 2.
   *
   * @group expertSetParam
   */
  @Since("2.1.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)
  setDefault(aggregationDepth -> 2)

  /**
   * Set the lower bounds on coefficients if fitting under bound constrained optimization.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setLowerBoundsOnCoefficients(value: Matrix): this.type = set(lowerBoundsOnCoefficients, value)

  /**
   * Set the upper bounds on coefficients if fitting under bound constrained optimization.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setUpperBoundsOnCoefficients(value: Matrix): this.type = set(upperBoundsOnCoefficients, value)

  /**
   * Set the lower bounds on intercepts if fitting under bound constrained optimization.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setLowerBoundsOnIntercepts(value: Vector): this.type = set(lowerBoundsOnIntercepts, value)

  /**
   * Set the upper bounds on intercepts if fitting under bound constrained optimization.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setUpperBoundsOnIntercepts(value: Vector): this.type = set(upperBoundsOnIntercepts, value)

  private def assertBoundConstrainedOptimizationParamsValid(
      numCoefficientSets: Int,
      numFeatures: Int): Unit = {
    if (isSet(lowerBoundsOnCoefficients)) {
      require($(lowerBoundsOnCoefficients).numRows == numCoefficientSets &&
        $(lowerBoundsOnCoefficients).numCols == numFeatures,
        "The shape of LowerBoundsOnCoefficients must be compatible with (1, number of features) " +
          "for binomial regression, or (number of classes, number of features) for multinomial " +
          "regression, but found: " +
          s"(${getLowerBoundsOnCoefficients.numRows}, ${getLowerBoundsOnCoefficients.numCols}).")
    }
    if (isSet(upperBoundsOnCoefficients)) {
      require($(upperBoundsOnCoefficients).numRows == numCoefficientSets &&
        $(upperBoundsOnCoefficients).numCols == numFeatures,
        "The shape of upperBoundsOnCoefficients must be compatible with (1, number of features) " +
          "for binomial regression, or (number of classes, number of features) for multinomial " +
          "regression, but found: " +
          s"(${getUpperBoundsOnCoefficients.numRows}, ${getUpperBoundsOnCoefficients.numCols}).")
    }
    if (isSet(lowerBoundsOnIntercepts)) {
      require($(lowerBoundsOnIntercepts).size == numCoefficientSets, "The size of " +
        "lowerBoundsOnIntercepts must be equal to 1 for binomial regression, or the number of " +
        s"classes for multinomial regression, but found: ${getLowerBoundsOnIntercepts.size}.")
    }
    if (isSet(upperBoundsOnIntercepts)) {
      require($(upperBoundsOnIntercepts).size == numCoefficientSets, "The size of " +
        "upperBoundsOnIntercepts must be equal to 1 for binomial regression, or the number of " +
        s"classes for multinomial regression, but found: ${getUpperBoundsOnIntercepts.size}.")
    }
    if (isSet(lowerBoundsOnCoefficients) && isSet(upperBoundsOnCoefficients)) {
      require($(lowerBoundsOnCoefficients).toArray.zip($(upperBoundsOnCoefficients).toArray)
        .forall(x => x._1 <= x._2), "LowerBoundsOnCoefficients should always be " +
        "less than or equal to upperBoundsOnCoefficients, but found: " +
        s"lowerBoundsOnCoefficients = $getLowerBoundsOnCoefficients, " +
        s"upperBoundsOnCoefficients = $getUpperBoundsOnCoefficients.")
    }
    if (isSet(lowerBoundsOnIntercepts) && isSet(upperBoundsOnIntercepts)) {
      require($(lowerBoundsOnIntercepts).toArray.zip($(upperBoundsOnIntercepts).toArray)
        .forall(x => x._1 <= x._2), "LowerBoundsOnIntercepts should always be " +
        "less than or equal to upperBoundsOnIntercepts, but found: " +
        s"lowerBoundsOnIntercepts = $getLowerBoundsOnIntercepts, " +
        s"upperBoundsOnIntercepts = $getUpperBoundsOnIntercepts.")
    }
  }

  private var optInitialModel: Option[LogisticRegressionModel] = None

  private[spark] def setInitialModel(model: LogisticRegressionModel): this.type = {
    this.optInitialModel = Some(model)
    this
  }

  override protected[spark] def train(dataset: Dataset[_]): LogisticRegressionModel = {
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    train(dataset, handlePersistence)
  }

  protected[spark] def train(
      dataset: Dataset[_],
      handlePersistence: Boolean): LogisticRegressionModel = {
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] =
      dataset.select(col($(labelCol)), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Instance(label, weight, features)
      }

    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val instr = Instrumentation.create(this, instances)
    instr.logParams(regParam, elasticNetParam, standardization, threshold,
      maxIter, tol, fitIntercept)

    val (summarizer, labelSummarizer) = {
      val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        instance: Instance) =>
        (c._1.add(instance.features, instance.weight), c._2.add(instance.label, instance.weight))

      val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        c2: (MultivariateOnlineSummarizer, MultiClassSummarizer)) =>
        (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        (new MultivariateOnlineSummarizer, new MultiClassSummarizer)
      )(seqOp, combOp, $(aggregationDepth))
    }

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numFeatures = summarizer.mean.size
    val numFeaturesPlusIntercept = if (getFitIntercept) numFeatures + 1 else numFeatures

    val numClasses = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) =>
        require(n >= histogram.length, s"Specified number of classes $n was " +
          s"less than the number of unique labels ${histogram.length}.")
        n
      case None => histogram.length
    }

    val isMultinomial = getFamily.toLowerCase(Locale.ROOT) match {
      case "binomial" =>
        require(numClasses == 1 || numClasses == 2, s"Binomial family only supports 1 or 2 " +
          s"outcome classes but found $numClasses.")
        false
      case "multinomial" => true
      case "auto" => numClasses > 2
      case other => throw new IllegalArgumentException(s"Unsupported family: $other")
    }
    val numCoefficientSets = if (isMultinomial) numClasses else 1

    // Check params interaction is valid if fitting under bound constrained optimization.
    if (usingBoundConstrainedOptimization) {
      assertBoundConstrainedOptimizationParamsValid(numCoefficientSets, numFeatures)
    }

    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)

    val (coefficientMatrix, interceptVector, objectiveHistory) = {
      if (numInvalid != 0) {
        val msg = s"Classification labels should be in [0 to ${numClasses - 1}]. " +
          s"Found $numInvalid invalid labels."
        logError(msg)
        throw new SparkException(msg)
      }

      val isConstantLabel = histogram.count(_ != 0.0) == 1

      if ($(fitIntercept) && isConstantLabel && !usingBoundConstrainedOptimization) {
        logWarning(s"All labels are the same value and fitIntercept=true, so the coefficients " +
          s"will be zeros. Training is not needed.")
        val constantLabelIndex = Vectors.dense(histogram).argmax
        val coefMatrix = new SparseMatrix(numCoefficientSets, numFeatures,
          new Array[Int](numCoefficientSets + 1), Array.empty[Int], Array.empty[Double],
          isTransposed = true).compressed
        val interceptVec = if (isMultinomial) {
          Vectors.sparse(numClasses, Seq((constantLabelIndex, Double.PositiveInfinity)))
        } else {
          Vectors.dense(if (numClasses == 2) Double.PositiveInfinity else Double.NegativeInfinity)
        }
        (coefMatrix, interceptVec, Array.empty[Double])
      } else {
        if (!$(fitIntercept) && isConstantLabel) {
          logWarning(s"All labels belong to a single class and fitIntercept=false. It's a " +
            s"dangerous ground, so the algorithm may not converge.")
        }

        val featuresMean = summarizer.mean.toArray
        val featuresStd = summarizer.variance.toArray.map(math.sqrt)

        if (!$(fitIntercept) && (0 until numFeatures).exists { i =>
          featuresStd(i) == 0.0 && featuresMean(i) != 0.0 }) {
          logWarning("Fitting LogisticRegressionModel without intercept on dataset with " +
            "constant nonzero column, Spark MLlib outputs zero coefficients for constant " +
            "nonzero columns. This behavior is the same as R glmnet but different from LIBSVM.")
        }

        val regParamL1 = $(elasticNetParam) * $(regParam)
        val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

        val bcFeaturesStd = instances.context.broadcast(featuresStd.map(t =>
          if (t != 0d) 1d / t else 0d))
        val getAggregatorFunc = new LogisticAggregatorX(bcFeaturesStd, numClasses, $(fitIntercept),
          multinomial = isMultinomial)(_)
        val getFeaturesStd = (j: Int) => if (j >= 0 && j < numCoefficientSets * numFeatures) {
          featuresStd(j / numCoefficientSets)
        } else {
          0.0
        }

        val regularization = if (regParamL2 != 0.0) {
          val shouldApply = (idx: Int) => idx >= 0 && idx < numFeatures * numCoefficientSets
          Some(new L2Regularization(regParamL2, shouldApply,
            if ($(standardization)) None else Some(getFeaturesStd)))
        } else {
          None
        }

        val costFun = new RDDLossFunctionX(instances, getAggregatorFunc, regularization,
          $(aggregationDepth))

        val numCoeffsPlusIntercepts = numFeaturesPlusIntercept * numCoefficientSets

        val (lowerBounds, upperBounds): (Array[Double], Array[Double]) = {
          if (usingBoundConstrainedOptimization) {
            val lowerBounds = Array.fill[Double](numCoeffsPlusIntercepts)(Double.MinValue)
            val upperBounds = Array.fill[Double](numCoeffsPlusIntercepts)(Double.MaxValue)
            val isSetLowerBoundsOnCoefficients = isSet(lowerBoundsOnCoefficients)
            val isSetUpperBoundsOnCoefficients = isSet(upperBoundsOnCoefficients)
            val isSetLowerBoundsOnIntercepts = isSet(lowerBoundsOnIntercepts)
            val isSetUpperBoundsOnIntercepts = isSet(upperBoundsOnIntercepts)

            var i = 0
            while (i < numCoeffsPlusIntercepts) {
              val coefficientSetIndex = i % numCoefficientSets
              val featureIndex = i / numCoefficientSets
              if (featureIndex < numFeatures) {
                if (isSetLowerBoundsOnCoefficients) {
                  lowerBounds(i) = $(lowerBoundsOnCoefficients)(
                    coefficientSetIndex, featureIndex) * featuresStd(featureIndex)
                }
                if (isSetUpperBoundsOnCoefficients) {
                  upperBounds(i) = $(upperBoundsOnCoefficients)(
                    coefficientSetIndex, featureIndex) * featuresStd(featureIndex)
                }
              } else {
                if (isSetLowerBoundsOnIntercepts) {
                  lowerBounds(i) = $(lowerBoundsOnIntercepts)(coefficientSetIndex)
                }
                if (isSetUpperBoundsOnIntercepts) {
                  upperBounds(i) = $(upperBoundsOnIntercepts)(coefficientSetIndex)
                }
              }
              i += 1
            }
            (lowerBounds, upperBounds)
          } else {
            (null, null)
          }
        }

        val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
          if (lowerBounds != null && upperBounds != null) {
            new LBFGSL(BDV[Double](lowerBounds), BDV[Double](upperBounds), $(maxIter), 10, $(tol))
          } else {
            new LBFGSL($(maxIter), 10, $(tol))
          }
        } else {
          val standardizationParam = $(standardization)
          val effectiveL1Reg = Array.fill[Double](numCoeffsPlusIntercepts)(0d)
            .zipWithIndex.map{case (_, index) =>
            // Remove the L1 penalization on the intercept
            val isIntercept = $(fitIntercept) && index >= numFeatures * numCoefficientSets
            if (isIntercept) {
              0.0
            } else {
              if (standardizationParam) {
                regParamL1
              } else {
                val featureIndex = index / numCoefficientSets
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                if (featuresStd(featureIndex) != 0.0) {
                  regParamL1 / featuresStd(featureIndex)
                } else {
                  0.0
                }
              }
            }
          }
          new OWLQNL($(maxIter), 10, $(tol), BDV[Double](effectiveL1Reg))
        }

        /*
          The coefficients are laid out in column major order during training. Here we initialize
          a column major matrix of initial coefficients.
         */
        val initialCoefWithInterceptMatrix =
          Matrices.zeros(numCoefficientSets, numFeaturesPlusIntercept)

        val initialModelIsValid = optInitialModel match {
          case Some(_initialModel) =>
            val providedCoefs = _initialModel.coefficientMatrix
            val modelIsValid = (providedCoefs.numRows == numCoefficientSets) &&
              (providedCoefs.numCols == numFeatures) &&
              (_initialModel.interceptVector.size == numCoefficientSets) &&
              (_initialModel.getFitIntercept == $(fitIntercept))
            if (!modelIsValid) {
              logWarning(s"Initial coefficients will be ignored! Its dimensions " +
                s"(${providedCoefs.numRows}, ${providedCoefs.numCols}) did not match the " +
                s"expected size ($numCoefficientSets, $numFeatures)")
            }
            modelIsValid
          case None => false
        }

        if (initialModelIsValid) {
          val providedCoef = optInitialModel.get.coefficientMatrix
          providedCoef.foreachActive { (classIndex, featureIndex, value) =>
            // We need to scale the coefficients since they will be trained in the scaled space
            initialCoefWithInterceptMatrix.update(classIndex, featureIndex,
              value * featuresStd(featureIndex))
          }
          if ($(fitIntercept)) {
            optInitialModel.get.interceptVector.foreachActive { (classIndex, value) =>
              initialCoefWithInterceptMatrix.update(classIndex, numFeatures, value)
            }
          }
        } else if ($(fitIntercept) && isMultinomial) {
          /*
             For multinomial logistic regression, when we initialize the coefficients as zeros,
             it will converge faster if we initialize the intercepts such that
             it follows the distribution of the labels.
             {{{
               P(1) = \exp(b_1) / Z
               ...
               P(K) = \exp(b_K) / Z
               where Z = \sum_{k=1}^{K} \exp(b_k)
             }}}
             Since this doesn't have a unique solution, one of the solutions that satisfies the
             above equations is
             {{{
               \exp(b_k) = count_k * \exp(\lambda)
               b_k = \log(count_k) * \lambda
             }}}
             \lambda is a free parameter, so choose the phase \lambda such that the
             mean is centered. This yields
             {{{
               b_k = \log(count_k)
               b_k' = b_k - \mean(b_k)
             }}}
           */
          val rawIntercepts = histogram.map(math.log1p) // add 1 for smoothing (log1p(x) = log(1+x))
          val rawMean = rawIntercepts.sum / rawIntercepts.length
          rawIntercepts.indices.foreach { i =>
            initialCoefWithInterceptMatrix.update(i, numFeatures, rawIntercepts(i) - rawMean)
          }
        } else if ($(fitIntercept)) {
          /*
             For binary logistic regression, when we initialize the coefficients as zeros,
             it will converge faster if we initialize the intercept such that
             it follows the distribution of the labels.

             {{{
               P(0) = 1 / (1 + \exp(b)), and
               P(1) = \exp(b) / (1 + \exp(b))
             }}}, hence
             {{{
               b = \log{P(1) / P(0)} = \log{count_1 / count_0}
             }}}
           */
          initialCoefWithInterceptMatrix.update(0, numFeatures,
            math.log(histogram(1) / histogram(0)))
        }

        if (usingBoundConstrainedOptimization) {
          // Make sure all initial values locate in the corresponding bound.
          var i = 0
          while (i < numCoeffsPlusIntercepts) {
            val coefficientSetIndex = i % numCoefficientSets
            val featureIndex = i / numCoefficientSets
            if (initialCoefWithInterceptMatrix(coefficientSetIndex, featureIndex) < lowerBounds(i))
            {
              initialCoefWithInterceptMatrix.update(
                coefficientSetIndex, featureIndex, lowerBounds(i))
            } else if (
              initialCoefWithInterceptMatrix(coefficientSetIndex, featureIndex) > upperBounds(i))
            {
              initialCoefWithInterceptMatrix.update(
                coefficientSetIndex, featureIndex, upperBounds(i))
            }
            i += 1
          }
        }

        val states = optimizer.iterations(new CachedDiffFunction(costFun),
          new BDV[Double](initialCoefWithInterceptMatrix.toArray))

        /*
           Note that in Logistic Regression, the objective history (loss + regularization)
           is log-likelihood which is invariant under feature standardization. As a result,
           the objective history from optimizer is the same as the one in the original space.
         */
        val arrayBuilder = mutable.ArrayBuilder.make[Double]
        var state: optimizer.State = null
        while (states.hasNext) {
          state = states.next()
          arrayBuilder += state.adjustedValue
        }
        bcFeaturesStd.destroy(blocking = false)

        if (state == null) {
          val msg = s"${optimizer.getClass.getName} failed."
          logError(msg)
          throw new SparkException(msg)
        }

        /*
           The coefficients are trained in the scaled space; we're converting them back to
           the original space.

           Additionally, since the coefficients were laid out in column major order during training
           to avoid extra computation, we convert them back to row major before passing them to the
           model.

           Note that the intercept in scaled space and original space is the same;
           as a result, no scaling is needed.
         */
        val allCoefficients = state.x.toArray.clone()
        val allCoefMatrix = new DenseMatrix(numCoefficientSets, numFeaturesPlusIntercept,
          allCoefficients)
        val denseCoefficientMatrix = new DenseMatrix(numCoefficientSets, numFeatures,
          new Array[Double](numCoefficientSets * numFeatures), isTransposed = true)
        val interceptVec = if ($(fitIntercept) || !isMultinomial) {
          Vectors.zeros(numCoefficientSets)
        } else {
          Vectors.sparse(numCoefficientSets, Seq.empty)
        }
        // separate intercepts and coefficients from the combined matrix
        allCoefMatrix.foreachActive { (classIndex, featureIndex, value) =>
          val isIntercept = $(fitIntercept) && (featureIndex == numFeatures)
          if (!isIntercept && featuresStd(featureIndex) != 0.0) {
            denseCoefficientMatrix.update(classIndex, featureIndex,
              value / featuresStd(featureIndex))
          }
          if (isIntercept) interceptVec.toArray(classIndex) = value
        }

        if ($(regParam) == 0.0 && isMultinomial && !usingBoundConstrainedOptimization) {
          /*
            When no regularization is applied, the multinomial coefficients lack identifiability
            because we do not use a pivot class. We can add any constant value to the coefficients
            and get the same likelihood. So here, we choose the mean centered coefficients for
            reproducibility. This method follows the approach in glmnet, described here:

            Friedman, et al. "Regularization Paths for Generalized Linear Models via
              Coordinate Descent," https://core.ac.uk/download/files/153/6287975.pdf
           */
          val centers = Array.fill(numFeatures)(0.0)
          denseCoefficientMatrix.foreachActive { case (i, j, v) =>
            centers(j) += v
          }
          centers.transform(_ / numCoefficientSets)
          denseCoefficientMatrix.foreachActive { case (i, j, v) =>
            denseCoefficientMatrix.update(i, j, v - centers(j))
          }
        }

        // center the intercepts when using multinomial algorithm
        if ($(fitIntercept) && isMultinomial && !usingBoundConstrainedOptimization) {
          val interceptArray = interceptVec.toArray
          val interceptMean = interceptArray.sum / interceptArray.length
          (0 until interceptVec.size).foreach { i => interceptArray(i) -= interceptMean }
        }
        (denseCoefficientMatrix.compressed, interceptVec.compressed, arrayBuilder.result())
      }
    }

    if (handlePersistence) instances.unpersist()

    val model = copyValues(new LogisticRegressionModel(uid, coefficientMatrix, interceptVector,
      numClasses, isMultinomial))

    val (summaryModel, probabilityColName, predictionColName) = model.findSummaryModel()
    val logRegSummary = if (numClasses <= 2) {
      new BinaryLogisticRegressionTrainingSummaryImpl(
        summaryModel.transform(dataset),
        probabilityColName,
        predictionColName,
        $(labelCol),
        $(featuresCol),
        objectiveHistory)
    } else {
      new LogisticRegressionTrainingSummaryImpl(
        summaryModel.transform(dataset),
        probabilityColName,
        predictionColName,
        $(labelCol),
        $(featuresCol),
        objectiveHistory)
    }
    model.setSummary(Some(logRegSummary))
    instr.logSuccess(model)
    model
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): LogisticRegression = defaultCopy(extra)
}

@Since("1.6.0")
object LogisticRegression extends DefaultParamsReadable[LogisticRegression] {

  @Since("1.6.0")
  override def load(path: String): LogisticRegression = super.load(path)

  private[classification] val supportedFamilyNames =
    Array("auto", "binomial", "multinomial").map(_.toLowerCase(Locale.ROOT))
}
