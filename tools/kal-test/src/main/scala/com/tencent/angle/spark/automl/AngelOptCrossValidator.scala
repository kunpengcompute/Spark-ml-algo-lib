// scalastyle:off
package com.tencent.angel.spark.automl

import com.tencent.angel.spark.automl.tuner.TunerParam
import com.tencent.angel.spark.automl.tuner.config.Configuration
import com.tencent.angel.spark.automl.tuner.parameter.ParamSpace
import com.tencent.angel.spark.automl.tuner.solver.Solver
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.PublicThreadUtils
import org.json4s.{DefaultFormats, JObject, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

private[spark] trait AngelOptCrossValidatorParams extends Params {
  protected def transformSchemaImpl(schema: StructType): StructType = {
    $(estimator).transformSchema(schema)
  }

  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")
  def getEstimator: Estimator[_] = $(estimator)

  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")
  def getEvaluator: Evaluator = $(evaluator)

  val numFolds: IntParam = new IntParam(
    this,
    "numFolds",
    "number of folds for cross validation (>= 2)",
    ParamValidators.gtEq(2)
  )

  def getNumFolds: Int = $(numFolds)

  val numIterations: IntParam = new IntParam(
    this,
    "numIterations",
    "number of cross validations to run (>= 2)",
    ParamValidators.gtEq(2)
  )

  def getNumIterations: Int = $(numIterations)

  setDefault(numFolds -> 3)
  setDefault(numIterations -> 10)

  final val seed: LongParam = new LongParam(this, "seed", "random seed")

  setDefault(seed, this.getClass.getName.hashCode.toLong)

  /** @group getParam */
  final def getSeed: Long = $(seed)

  protected final val surrogateName = new Param[String](this, "surrogateName", "tuning method, such as GaussianProcess, RandomForest, Random, Grid")
  def getSurrogate: String = $(surrogateName)
  setDefault(surrogateName -> "RandomForest")

  val estimatorParamSpace: Param[Array[ParamSpace[Double]]] =
    new Param(this, "estimatorParamSpace", "hyper-parameters space for the estimator")

  def getEstimatorParamSpace: Array[ParamSpace[Double]] = $(estimatorParamSpace)

  val parallelism = new IntParam(this, "parallelism",
    "the number of threads to use when running parallel algorithms", ParamValidators.gtEq(1))

  val threshold = new DoubleParam(this, "threshold",
    "threshold to stop working")

  setDefault(parallelism -> 1)
  setDefault(threshold -> 0.0)

  val thresholdFlag = new BooleanParam(this, "thresholdFlag",
    "flag for threshold judgment")

  /** @group getParam */
  def getThresholdFlag: Boolean = $(thresholdFlag)

  setDefault(thresholdFlag -> false)

  /** @group expertGetParam */
  def getParallelism: Int = $(parallelism)

  def getExecutionContext: ExecutionContext = {
    getParallelism match {
      case 1 =>
        PublicThreadUtils.utils.sameThread
      case n =>
        ExecutionContext.fromExecutorService(PublicThreadUtils.utils
          .newDaemonCachedThreadPool(s"${this.getClass.getSimpleName}-thread-pool", n))
    }
  }
}

class AngelOptCrossValidator (override val uid: String)
  extends AngelOptCrossValidatorParams {
  def this() = this(Identifiable.randomUID("cv"))

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  def setNumIterations(value: Int): this.type = set(numIterations, value)

  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  def setSurrogate(value: String): this.type = set(surrogateName, value)

  /** @group setParam */
  def setEstimatorParamSpace(value: Array[ParamSpace[Double]]): this.type = set(estimatorParamSpace, value)

  def setParallelism(value: Int): this.type = set(parallelism, value)

  def setThreshold(value: Double): this.type = {
    set(threshold, value)
    set(thresholdFlag, true)
  }

  def setThresholdFlag(value: Boolean): this.type = set(thresholdFlag, true)

  private def isStop(metric: Double): Boolean = {
    if ($(evaluator).isLargerBetter && metric >= $(threshold)) {
      true
    } else if (!$(evaluator).isLargerBetter && metric <= $(threshold)) {
      true
    } else {
      false
    }
  }

  var searchNumber: Int = 0
  var bestMetric: Double = 0.0

  def fit(dataset: Dataset[_]): AngelOptCrossValidatorModel = {
    val sqlContext = dataset.sqlContext
    val schema = dataset.schema
    val est = $(estimator)
    val eval = $(evaluator)
    val folds = $(numFolds)
    val iterations = $(numIterations)
    val paramList = $(estimatorParamSpace)
    val executionContext = getExecutionContext

    val solver = Solver(paramList,  !eval.isLargerBetter,  $(surrogateName))

    var stop = false
    val observations = for {
      iter <- (1 to iterations) if !stop
    } yield {
      val configs: Configuration = solver.suggest()(0)
      val epm: ParamMap = configs.getParamMapWithParent
      val splits = MLUtils.kFold(dataset.toDF.rdd, folds, $(seed))
      val accMetricsFuture = splits.map { case (training, validation) => Future[Double]{
        val trainingDataset = sqlContext.createDataFrame(training, schema)
        val validationDataset = sqlContext.createDataFrame(validation, schema)
        val models = est.fit(trainingDataset, epm)
        eval.evaluate((models.asInstanceOf[Model[_]]).transform(validationDataset, epm))
      }(executionContext)
      }
      val accMetrics = accMetricsFuture.map(PublicThreadUtils.utils.awaitResult(_, Duration.Inf))
      val avgMetric: Double = (accMetrics).sum / (accMetrics).length

      solver.feed(configs, avgMetric)

      println(iter, configs.getVector, avgMetric)

      if ($(thresholdFlag)) {
        stop = isStop(avgMetric)
      }

      (epm, avgMetric)
    }
    val bestObservation = if (eval.isLargerBetter) observations.maxBy(_._2) else observations.minBy(_._2)
    val bestModel = (est.fit(dataset, bestObservation._1)).asInstanceOf[Model[_]]
    searchNumber = observations.length
    bestMetric = bestObservation._2

    copyValues(new AngelOptCrossValidatorModel(uid, bestModel, searchNumber, bestMetric))
  }

  override def copy(extra: ParamMap): AngelOptCrossValidator = {
    val copied = defaultCopy(extra).asInstanceOf[AngelOptCrossValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    if (copied.isDefined(numFolds)) {
      copied.setNumFolds(copied.getNumFolds)
    }
    if (copied.isDefined(numIterations)) {
      copied.setNumIterations(copied.getNumIterations)
    }
    copied
  }
}

class AngelOptCrossValidatorModel private[spark] (
                                                 override val uid: String,
                                                 val bestModel: Model[_],
                                                 val searchNumber: Int,
                                                 val bestMetric: Double
                                               ) extends Model[AngelOptCrossValidatorModel] with AngelOptCrossValidatorParams {
  override def copy(extra: ParamMap): AngelOptCrossValidatorModel = {
    val copied = new AngelOptCrossValidatorModel(uid, bestModel.copy(extra).asInstanceOf[Model[_]], searchNumber, bestMetric)
    copyValues(copied, extra).setParent(parent)
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, true)
    bestModel.transform(dataset)
  }
  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }
}