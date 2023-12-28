// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package com.huawei.bigdata.alogrithms.isolationforest

import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType


trait IsolationForestParams extends Params {
  def setNumTrees(value: Int): this.type = null
  def setMaxInstances(value: Double): this.type = null
  def setAnomalyRatio(value: Double): this.type = null
  def setMaxFea(value: Double): this.type = null
  def setBootstrap(value: Boolean): this.type = null
  def setAnomalyRatioError(value: Double): this.type = null
  def setRandomSeed(value: Long): this.type = null
  def setFeaturesCol(value: String): this.type = null
  def setPredictionCol(value: String): this.type = null
  def setScoreCol(value: String): this.type = null
}

class Tree() extends Serializable {
}

class IsolationForestModel(
    override val uid: String,
    val trees: Array[Tree],
    private val numSamples: Int)
  extends Model[IsolationForestModel] with IsolationForestParams {
  override def copy(extra: ParamMap): IsolationForestModel = null
  override def transform(dataset: Dataset[_]): DataFrame = null
  override def transformSchema(schema: StructType): StructType = null
}

object IsolationForestModel {
  def saveModel(model: IsolationForestModel, path: String): Unit = {}
  def loadModel(path: String): IsolationForestModel = null
}

class IsolationForest(override val uid: String) extends Estimator[IsolationForestModel]
  with IsolationForestParams with DefaultParamsWritable with Logging {
  def this() = this(Identifiable.randomUID("if"))
  override def fit(dataset: Dataset[_]): IsolationForestModel = null
  override def transformSchema(schema: StructType): StructType = null
  override def copy(extra: ParamMap): IsolationForest = null
}
