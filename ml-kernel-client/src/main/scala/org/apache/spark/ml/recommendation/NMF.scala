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
//
package org.apache.spark.ml.recommendation

import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasBlockSize, HasCheckpointInterval, HasMaxIter, HasPredictionCol, HasRegParam, HasSeed}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType


/**
 * Common params for NMF and NMFModel.
 */
private[recommendation] trait NMFModelParams extends Params with HasPredictionCol
  with HasBlockSize {

}

/**
 * Common params for NMF.
 */
private[recommendation] trait NMFParams extends NMFModelParams with HasMaxIter with HasRegParam
  with HasCheckpointInterval with HasSeed {

}

class NMFModel private[ml](
  override val uid: String,
  val rank: Int,
  @transient val userFactors: DataFrame,
  @transient val itemFactors: DataFrame)
  extends Model[NMFModel] with NMFModelParams with MLWritable {
  /** @group setParam */
  def setUserCol(value: String): this.type = null

  /** @group setParam */
  def setItemCol(value: String): this.type = null

  /** @group setParam */
  def setPredictionCol(value: String): this.type = null

  /** @group expertSetParam */
  def setColdStartStrategy(value: String): this.type = null

  /**
   * Set block size for stacking input data in matrices.
   * Default is 4096.
   *
   * @group expertSetParam
   */
  def setBlockSize(value: Int): this.type = null

  override def transform(dataset: Dataset[_]): DataFrame = null

  override def transformSchema(schema: StructType): StructType = null

  override def copy(extra: ParamMap): NMFModel = null

  override def write: MLWriter = null

  override def toString: String = null

  /**
   * Returns top `numItems` items recommended for each user, for all users.
   *
   * @param numItems max number of recommendations for each user
   * @return a DataFrame of (userCol: Int, recommendations), where recommendations are
   *         stored as an array of (itemCol: Int, rating: Float) Rows.
   */
  def recommendForAllUsers(numItems: Int): DataFrame = null

  /**
   * Returns top `numItems` items recommended for each user id in the input data set. Note that if
   * there are duplicate ids in the input dataset, only one set of recommendations per unique id
   * will be returned.
   *
   * @param dataset  a Dataset containing a column of user ids.
   *                 The column name must match `userCol`.
   * @param numItems max number of recommendations for each user.
   * @return a DataFrame of (userCol: Int, recommendations), where recommendations are
   *         stored as an array of (itemCol: Int, rating: Float) Rows.
   */
  def recommendForUserSubset(dataset: Dataset[_], numItems: Int): DataFrame = null

  /**
   * Returns top `numUsers` users recommended for each item, for all items.
   *
   * @param numUsers max number of recommendations for each item
   * @return a DataFrame of (itemCol: Int, recommendations), where recommendations are
   *         stored as an array of (userCol: Int, rating: Float) Rows.
   */
  def recommendForAllItems(numUsers: Int): DataFrame = null

  /**
   * Returns top `numUsers` users recommended for each item id in the input data set. Note that if
   * there are duplicate ids in the input dataset, only one set of recommendations per unique id
   * will be returned.
   *
   * @param dataset  a Dataset containing a column of item ids.
   *                 The column name must match `itemCol`.
   * @param numUsers max number of recommendations for each item.
   * @return a DataFrame of (itemCol: Int, recommendations), where recommendations are
   *         stored as an array of (userCol: Int, rating: Float) Rows.
   */
  def recommendForItemSubset(dataset: Dataset[_], numUsers: Int): DataFrame = null
}

class NMF(override val uid: String) extends Estimator[NMFModel] with NMFParams
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("nmf"))

  /** @group setParam */
  def setRank(value: Int): this.type = null

  /** @group setParam */
  def setNumUserBlocks(value: Int): this.type = null

  /** @group setParam */
  def setNumItemBlocks(value: Int): this.type = null

  /** @group setParam */
  def setUserCol(value: String): this.type = null

  /** @group setParam */
  def setItemCol(value: String): this.type = null

  /** @group setParam */
  def setRatingCol(value: String): this.type = null

  /** @group setParam */
  def setPredictionCol(value: String): this.type = null

  /** @group setParam */
  def setMaxIter(value: Int): this.type = null

  /** @group setParam */
  def setRegParam(value: Double): this.type = null

  /** @group setParam */
  def setCheckpointInterval(value: Int): this.type = null

  /** @group setParam */
  def setSeed(value: Long): this.type = null

  /** @group expertSetParam */
  def setIntermediateStorageLevel(value: String): this.type = null

  /** @group expertSetParam */
  def setFinalStorageLevel(value: String): this.type = null

  /** @group expertSetParam */
  def setColdStartStrategy(value: String): this.type = null

  /**
   * Set block size for stacking input data in matrices.
   * Default is 4096.
   *
   * @group expertSetParam
   */
  def setBlockSize(value: Int): this.type = null

  /**
   * Sets both numUserBlocks and numItemBlocks to the specific value.
   *
   * @group setParam
   */
  def setNumBlocks(value: Int): this.type = {
    setNumUserBlocks(value)
    setNumItemBlocks(value)
    this
  }

  override def fit(dataset: Dataset[_]): NMFModel = null

  override def transformSchema(schema: StructType): StructType = null

  override def copy(extra: ParamMap): NMF = null
}

object NMF extends DefaultParamsReadable[NMF] with Logging {
  /**
   * Rating class for better code readability.
   */
  case class Rating[@specialized(Int, Long) ID](user: ID, item: ID, rating: Float)
}

object NMFSolver {
  private[recommendation] def updateFactors[ID](
    dstIds: Array[ID],
    srcPtrs: Array[Int],
    srcEncodedIndices: Array[Int],
    ratings: Array[Float],
    srcFactors: Iterable[(Int, Array[Array[Float]])],
    numSrcBlocks: Int,
    rank: Int,
    regParam: Float,
    srcEncoder: NMFLocalIndexEncoder): Array[Array[Float]] = null
}
