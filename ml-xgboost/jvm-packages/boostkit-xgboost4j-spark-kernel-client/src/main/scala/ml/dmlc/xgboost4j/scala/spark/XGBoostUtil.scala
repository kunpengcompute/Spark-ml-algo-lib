// scalastyle:off header.matches
/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package ml.dmlc.xgboost4j.scala.spark

import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.{LabeledPoint => XGBLabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object XGBoostUtil {

  def checkIfCacheData(cacheTrainingSet: Boolean, params: Map[String, Any]): Boolean = {
    true
  }

  def trainDistributedKernel(
      params: Map[String, Any],
      xgbExecParams: XGBoostExecutionParams,
      rabitParams: java.util.Map[String, String],
      hasGroup: Boolean,
      sc: SparkContext,
      trainingData: Either[RDD[Array[XGBLabeledPoint]], RDD[XGBLabeledPoint]],
      evalSetsMap: Map[String, RDD[XGBLabeledPoint]],
      prevBooster: Booster): (Booster, Map[String, Array[Float]]) = {
    null
  }
}
