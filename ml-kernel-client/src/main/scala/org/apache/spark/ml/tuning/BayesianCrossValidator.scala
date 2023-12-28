// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.tuning

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasParallelism
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class BayesianCrossValidator(override val uid: String)
  extends Estimator[BayesianCrossValidatorModel]
    with BayesianCrossValidatorParams with HasParallelism {

  def this() = this(Identifiable.randomUID("bayesCv"))

  override def transformSchema(schema: StructType): StructType = null

  def setEstimator(value: Estimator[_]): this.type = null

  def setEvaluator(value: Evaluator): this.type = null

  def setNumFolds(value: Int): this.type = null

  def setNumIterations(value: Int): this.type = null

  def setSeed(value: Long): this.type = null

  def setEstimatorParamSpace(value: ParamSpace): this.type = null

  def setParallelism(value: Int): this.type = null

  def setThreshold(value: Double): this.type = null

  def setThresholdFlag(value: Boolean): this.type = null

  def getSearchNumber: Int = 0

  def getBestMetric: Double = 0.0

  override def fit(dataset: Dataset[_]): BayesianCrossValidatorModel = null

  override def copy(extra: ParamMap): BayesianCrossValidator = null
}

class BayesianCrossValidatorModel private[ml](
    override val uid: String,
    val bestModel: Model[_])
  extends Model[BayesianCrossValidatorModel] with BayesianCrossValidatorParams with MLWritable {

  override def write: MLWriter = null

  override def copy(extra: ParamMap): BayesianCrossValidatorModel = null

  override def transform(dataset: Dataset[_]): DataFrame = null

  override def transformSchema(schema: StructType): StructType = null
}
