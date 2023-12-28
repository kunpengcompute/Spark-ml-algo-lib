// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.neighbors

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.HasFeaturesCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType

trait KNNParams extends Params with HasFeaturesCol {}

class KNNModel(override val uid: String,
    val data: RDD[BatchData[Row]],
    val featureDim: Int) extends Model[KNNModel] with KNNParams {

  def setK(value: Int): this.type = null

  def setTestBatchSize(value: Int): this.type = null

  def setNeighborsCol(value: String): this.type = null

  def setDistanceCol(value: String): this.type = null

  override def transformSchema(schema: StructType): StructType = null

  override def copy(extra: ParamMap): KNNModel = null

  override def transform(dataset: Dataset[_]): DataFrame = null
}

class KNN(override val uid: String) extends Estimator[KNNModel] with KNNParams {

  def this() = this(Identifiable.randomUID("knn"))

  def setFeaturesCol(value: String): this.type = null

  def setAuxiliaryCols(value: Array[String]): this.type = null

  override def transformSchema(schema: StructType): StructType = null

  override def copy(extra: ParamMap): KNN = null

  override def fit(dataset: Dataset[_]): KNNModel = null
}
