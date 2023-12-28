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

import org.apache.hadoop.fs.Path
import org.json4s.{DefaultFormats, JObject}
import org.json4s.JsonDSL.list2jvalue
import org.json4s.jackson.JsonMethods.{parse, render}

import org.apache.spark.SparkContext
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{BooleanParam, DoubleParam, IntParam, Param, ParamPair, Params, ParamValidators}
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWriter, Instrumentation, MLWritable}
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.sql.types.StructType

/**
 * Params for BayesianCrossValidator.
 */
private[ml] trait BayesianCrossValidatorParams extends HasSeed with Params {

  /**
   * param for the estimator to be validated
   *
   * @group param
   */
  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")

  /** @group getParam */
  def getEstimator: Estimator[_] = $(estimator)

  /**
   * param for estimator param maps
   *
   * @group param
   */
  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")

  /** @group getParam */
  def getEvaluator: Evaluator = $(evaluator)

  /**
   * param for folds number
   *
   * @group param
   */
  val numFolds: IntParam = new IntParam(
    this,
    "numFolds",
    "number of folds for cross validation (>= 2)",
    ParamValidators.gtEq(2)
  )

  /** @group getParam */
  def getNumFolds: Int = $(numFolds)

  setDefault(numFolds -> 3)

  /**
   * param for iterations number
   *
   * @group param
   */
  val numIterations: IntParam = new IntParam(
    this,
    "numIterations",
    "number of cross validations to run (>= 2)",
    ParamValidators.gtEq(2)
  )

  /** @group getParam */
  def getNumIterations: Int = $(numIterations)

  setDefault(numIterations -> 10)

  /**
   * param for the estimator hyper-parameter space to be searched
   *
   * @group param
   */
  val estimatorParamSpace: Param[ParamSpace] =
    new Param(this, "estimatorParamSpace", "hyper-parameter space for the estimator")

  /** @group getParam */
  def getEstimatorParamSpace: ParamSpace = $(estimatorParamSpace)

  /**
   * param for the threshold to stop search
   *
   * @group param
   */
  val threshold = new DoubleParam(this, "threshold",
    "threshold to stop working")

  /** @group getParam */
  def getThreshold: Double = $(threshold)
  setDefault(threshold -> 0.0)

  /**
   * param for the threshold judgment flag
   *
   * @group param
   */
  val thresholdFlag = new BooleanParam(this, "thresholdFlag",
    "flag for threshold judgment")

  /** @group getParam */
  def getThresholdFlag: Boolean = $(thresholdFlag)

  setDefault(thresholdFlag -> false)

  protected def transformSchemaImpl(schema: StructType): StructType = {
    $(estimator).transformSchema(schema)
  }
}

private object BayesianCrossValidatorParams {

  /**
   * save method.
   *
   * @param path file path
   * @param instance BayesianCrossValidatorParams
   * @param sc SparkContext
   * @param extraMetadata extra metadata
   */
  def saveImpl(
                path: String,
                instance: BayesianCrossValidatorParams,
                sc: SparkContext,
                extraMetadata: Option[JObject] = None): Unit = {

    val params = instance.extractParamMap().toSeq
    val skipParams = List("estimator", "evaluator", "estimatorParamSpace")
    val jsonParams = render(params
      .filter { case ParamPair(p, v) => !skipParams.contains(p.name)}
      .map { case ParamPair(p, v) =>
        p.name -> parse(p.jsonEncode(v))
      }.toList)

    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

    val evaluatorPath = new Path(path, "evaluator").toString
    instance.getEvaluator.asInstanceOf[MLWritable].save(evaluatorPath)

    val estimatorPath = new Path(path, "estimator").toString
    instance.getEstimator.asInstanceOf[MLWritable].save(estimatorPath)
  }

  /**
   * load method.
   *
   * @param path file path
   * @param sc SparkContext
   * @param expectedClassName expected class name
   */
  def loadImpl[M <: Model[M]](
                               path: String,
                               sc: SparkContext,
                               expectedClassName: String): (Metadata, Estimator[M], Evaluator) = {

    val metadata = DefaultParamsReader.loadMetadata(path, sc, expectedClassName)

    implicit val format = DefaultFormats
    val evaluatorPath = new Path(path, "evaluator").toString
    val evaluator = DefaultParamsReader.loadParamsInstance[Evaluator](evaluatorPath, sc)
    val estimatorPath = new Path(path, "estimator").toString
    val estimator = DefaultParamsReader.loadParamsInstance[Estimator[M]](estimatorPath, sc)

    (metadata, estimator, evaluator)
  }
}
