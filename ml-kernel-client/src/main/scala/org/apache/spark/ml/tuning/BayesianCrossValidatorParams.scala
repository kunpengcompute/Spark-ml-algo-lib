// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.tuning

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.sql.types.StructType

private[ml] trait BayesianCrossValidatorParams extends HasSeed with Params {

  def getEstimator: Estimator[_] = null

  def getEvaluator: Evaluator = null

  def getNumFolds: Int = 0

  def getNumIterations: Int = 0

  def getEstimatorParamSpace: ParamSpace = null

  def getThreshold: Double = 0.0

  def getThresholdFlag: Boolean = true

  protected def transformSchemaImpl(schema: StructType): StructType = null
}
