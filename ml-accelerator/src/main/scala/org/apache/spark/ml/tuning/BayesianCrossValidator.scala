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

package org.apache.spark.ml.tuning

import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasParallelism
import org.apache.spark.ml.util.{DefaultParamsReader, Identifiable, MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils

/**
 * Bayesian Hyperparameter Optimization for K-fold cross validation
 */
class BayesianCrossValidator(override val uid: String)
  extends Estimator[BayesianCrossValidatorModel]
    with BayesianCrossValidatorParams with HasParallelism {

  def this() = this(Identifiable.randomUID("bayesCv"))

  /**
   * TransformSchema.
   * @param schema structType before
   * @return structType after
   */
  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  def setNumIterations(value: Int): this.type = set(numIterations, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  def setEstimatorParamSpace(value: ParamSpace): this.type = set(estimatorParamSpace, value)

  /** @group setParam */
  def setParallelism(value: Int): this.type = set(parallelism, value)

  /** @group setParam */
  def setThreshold(value: Double): this.type = {
    set(threshold, value)
    set(thresholdFlag, true)
  }

  /** @group setParam */
  def setThresholdFlag(value: Boolean): this.type = set(thresholdFlag, value)

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
  def getSearchNumber: Int = searchNumber

  var bestMetric: Double = 0.0
  def getBestMetric: Double = bestMetric

  /**
   * Fit the given dataset with BayesianCrossValidator.
   *
   * @param dataset dataset to be fitted
   * @return BayesianCrossValidatorModel
   */
  override def fit(dataset: Dataset[_]): BayesianCrossValidatorModel = {
    val sqlContext = dataset.sqlContext
    val schema = dataset.schema
    transformSchema(schema)
    val est = $(estimator)
    val eval = $(evaluator)
    val folds = $(numFolds)
    val iterations = $(numIterations)
    val paramSpace = $(estimatorParamSpace)
    val executionContext = getExecutionContext

    val solver = new Solver(sqlContext.sparkSession, paramSpace, !eval.isLargerBetter,
      BayesianCrossValidator.BATCH_SIZE, BayesianCrossValidator.SAMPLE_SIZE)
    val splits = MLUtils.kFold(dataset.toDF.rdd, folds, $(seed)).map {
      case (training, validation) =>
        val trainingDataset = sqlContext.createDataFrame(training, schema).persist()
        val validationDataset = sqlContext.createDataFrame(validation, schema).persist()
        (trainingDataset, validationDataset)
    }

    var stop = false
    val observations = for {
      iter <- (1 to iterations) if !stop
    } yield {
      val config: ParamMap = solver.suggest()(0)
      val accMetricsFuture = splits.map { case (training, validation) => Future[Double] {
        val models = est.fit(training, config)
        eval.evaluate((models.asInstanceOf[Model[_]]).transform(validation, config))
      }(executionContext)
      }
      val accMetrics = accMetricsFuture.map(ThreadUtils.awaitResult(_, Duration.Inf))
      val avgMetric: Double = (accMetrics).sum / (accMetrics).length
      logInfo(s"Iteration $iter: $avgMetric")
      solver.feed(config, avgMetric)
      if ($(thresholdFlag)) {
        stop = isStop(avgMetric)
      }
      (config, avgMetric)
    }

    splits.foreach {
      case (training, validation) =>
        training.unpersist()
        validation.unpersist()
    }

    val bestObservation =
      if (eval.isLargerBetter) observations.maxBy(_._2) else observations.minBy(_._2)
    val bestParams = bestObservation._1
    searchNumber = observations.length
    bestMetric = bestObservation._2

    logInfo(s"Best set of parameters:\n$bestParams")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = (est.fit(dataset, bestParams)).asInstanceOf[Model[_]]
    copyValues(
      new BayesianCrossValidatorModel(uid, bestModel).setParent(this))
  }

  /**
   * Copy a BayesianCrossValidator instance.
   * @param extra extra ParamMap
   * @return BayesianCrossValidator
   */
  override def copy(extra: ParamMap): BayesianCrossValidator = {
    val copied = defaultCopy(extra).asInstanceOf[BayesianCrossValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }
}

object BayesianCrossValidator {
  private val BATCH_SIZE: Int = 1
  private val SAMPLE_SIZE: Int = 10000
}

/**
 * BayesianCrossValidatorModel contains the bestModel.
 * @param uid uid of BayesianCrossValidatorModel
 * @param bestModel optimal metrics Model
 */
class BayesianCrossValidatorModel private[ml](
    override val uid: String,
    val bestModel: Model[_])
  extends Model[BayesianCrossValidatorModel] with BayesianCrossValidatorParams with MLWritable {

  /**
   * Write method.
   * @return MLWriter
   */
  override def write: MLWriter = new BayesianCrossValidatorModel
  .BayesianCrossValidatorModelWriter(this)

  /**
   * Copy a BayesianCrossValidatorModel instance.
   * @param extra extra ParamMap
   * @return BayesianCrossValidatorModel
   */
  override def copy(extra: ParamMap): BayesianCrossValidatorModel = {
    val copied = new BayesianCrossValidatorModel(uid, bestModel.copy(extra).asInstanceOf[Model[_]])
    copyValues(copied, extra).setParent(parent)
  }

  /**
   * Transform the dataset with best model.
   * @param dataset dataset to be transformed
   * @return transformed dataset
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  /**
   * TransformSchema
   * @param schema structType before
   * @return structType after
   */
  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }
}

object BayesianCrossValidatorModel extends MLReadable[BayesianCrossValidatorModel] {
  /**
   * Read method
   * @return MLReader[BayesianCrossValidatorModel]
   */
  override def read: MLReader[BayesianCrossValidatorModel] = new BayesianCrossValidatorModelReader

  /**
   * load method
   * @param path file path
   * @return BayesianCrossValidatorModel
   */
  override def load(path: String): BayesianCrossValidatorModel = super.load(path)

  private[BayesianCrossValidatorModel]
  class BayesianCrossValidatorModelWriter(instance: BayesianCrossValidatorModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      BayesianCrossValidatorParams.saveImpl(path, instance, sc)
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
    }
  }

  private class BayesianCrossValidatorModelReader extends MLReader[BayesianCrossValidatorModel] {

    private val className = classOf[BayesianCrossValidatorModel].getName

    override def load(path: String): BayesianCrossValidatorModel = {
      implicit val format: DefaultFormats.type = DefaultFormats

      val (metadata, estimator, evaluator) = BayesianCrossValidatorParams
        .loadImpl(path, sc, className)
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val model = new BayesianCrossValidatorModel(metadata.uid, bestModel)
      model.set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.numFolds, (metadata.params \ "numFolds").extract[Int])
        .set(model.numIterations, (metadata.params \ "numIterations").extract[Int])
    }
  }
}
