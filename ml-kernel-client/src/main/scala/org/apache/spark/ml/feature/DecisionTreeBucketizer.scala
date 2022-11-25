// scalastyle:off header.matches
/*
* Copyright (C) 2022. Huawei Technologies Co., Ltd.
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

package org.apache.spark.ml.feature

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.tree._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.tree.configuration.{Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType


private[ml] trait DecisionTreeBucketizerParams extends Params
  with DecisionTreeClassifierParams with HasWeightCol {

  /**
   * Param for bucketedFeatures column name.
   * @group param
   */
  final val bucketedFeaturesCol: Param[String] =
    new Param[String](this, "bucketedFeaturesCol", "bucketedFeatures column name")

  setDefault(bucketedFeaturesCol, "bucketedFeatures")

  /** @group getParam */
  final def getBucketedFeaturesCol: String = $(bucketedFeaturesCol)
}

/**
 * Decision tree bucketing algorithm for data discretization.
 */
@Since("1.4.0")
class DecisionTreeBucketizer @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends Estimator[DecisionTreeBucketModel]
  with DecisionTreeBucketizerParams with DecisionTreeClassifierParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("dtb"))

  def setLabelCol(value: String): this.type = null

  def setFeaturesCol(value: String): this.type = null

  def setBucketedFeaturesCol(value: String): this.type = null

  // Override parameter setters from parent trait for Java API compatibility.
  /** @group setParam */
  @Since("1.4.0")
  override def setMaxDepth(value: Int): this.type = null

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxBins(value: Int): this.type = null

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInstancesPerNode(value: Int): this.type = null

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInfoGain(value: Double): this.type = null

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setMaxMemoryInMB(value: Int): this.type = null

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setCacheNodeIds(value: Boolean): this.type = null

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * Must be at least 1.
   * (default = 10)
   * @group setParam
   */
  @Since("1.4.0")
  override def setCheckpointInterval(value: Int): this.type = null

  /** @group setParam */
  @Since("1.4.0")
  override def setImpurity(value: String): this.type = null

  /** @group setParam */
  @Since("1.6.0")
  override def setSeed(value: Long): this.type = null

  /** @group setParam */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Extract [[labelCol]] and [[featuresCol]] from the given dataset,
   * and put it in an RDD with strong types.
   *
   * @param dataset  DataFrame with columns for labels ([[org.apache.spark.sql.types.NumericType]])
   *                 and features (`Vector`).
   * @param numClasses  Number of classes label can take.  Labels must be integers in the range
   *                    [0, numClasses).
   * @note Throws `SparkException` if any label is a non-integer or is negative
   */
  private[ml] def extractLabeledPoints(dataset: Dataset[_], numClasses: Int): RDD[LabeledPoint] =
    null

  /**
   * Get the number of classes.  This looks in column metadata first, and if that is missing,
   * then this assumes classes are indexed 0,1,...,numClasses-1 and computes numClasses
   * by finding the maximum label value.
   *
   * Label validation (ensuring all labels are integers >= 0) needs to be handled elsewhere,
   * such as in `extractLabeledPoints()`.
   *
   * @param dataset  Dataset which contains a column [[labelCol]]
   * @param maxNumClasses  Maximum number of classes allowed when inferred from data.  If numClasses
   *                       is specified in the metadata, then maxNumClasses is ignored.
   * @return  number of classes
   * @throws IllegalArgumentException  if metadata does not specify numClasses, and the
   *                                   actual numClasses exceeds maxNumClasses
   */
  private[ml] def getNumClasses(dataset: Dataset[_], maxNumClasses: Int = 100): Int = 0

  override def transformSchema(schema: StructType): StructType = null

  override def fit(dataset: Dataset[_]): DecisionTreeBucketModel = null

  private[ml] def train(dataset: Dataset[_]): DecisionTreeBucketModel = null

  /** (private[ml]) Train decision trees on an RDD */
  private[ml] def train(data: RDD[LabeledPoint],
      oldStrategy: OldStrategy): DecisionTreeBucketModel = null

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): OldStrategy = null

  @Since("1.4.1")
  override def copy(extra: ParamMap): DecisionTreeBucketizer = null
}

@Since("1.4.0")
object DecisionTreeBucketizer extends DefaultParamsReadable[DecisionTreeBucketizer] {
  /** Accessor for supported impurities: entropy, gini */
  @Since("1.4.0")
  final val supportedImpurities: Array[String] = null

  @Since("2.0.0")
  override def load(path: String): DecisionTreeBucketizer = null
}

/**
 * Decision tree bucket model for data discretization.
 * @param _trees  Decision trees of all features.
 */
@Since("1.4.0")
class DecisionTreeBucketModel private[ml] (
    @Since("1.5.0") override val uid: String,
    private val _trees: Array[DecisionTreeClassificationModel],
    val numFeatures: Int,
    val numClasses: Int)
  extends Model[DecisionTreeBucketModel]
  with DecisionTreeBucketizerParams with DecisionTreeClassifierParams
  with TreeEnsembleModel[DecisionTreeClassificationModel]
  with MLWritable with Serializable {

  require(_trees.nonEmpty, "DecisionTreeBucketModel requires at least 1 tree.")

  /**
   * Construct a decision tree bucket model, with all trees weighted equally.
   *
   * @param trees  Component trees
   */
  private[ml] def this(
      trees: Array[DecisionTreeClassificationModel],
      numFeatures: Int,
      numClasses: Int) =
    this(Identifiable.randomUID("dtb"), trees, numFeatures, numClasses)

  def getNumTrees: Int = 0

  @Since("1.4.0")
  override def trees: Array[DecisionTreeClassificationModel] = null

  // Note: We may add support for weights (based on tree performance) later on.
  private lazy val _treeWeights: Array[Double] = null

  @Since("1.4.0")
  override def treeWeights: Array[Double] = null

  override def transformSchema(schema: StructType): StructType = null

  override def transform(dataset: Dataset[_]): DataFrame = null

  @Since("1.4.0")
  override def copy(extra: ParamMap): DecisionTreeBucketModel = null

  @Since("1.4.0")
  override def toString: String = null

  /** (private[ml]) Convert to a model in the old API */
  def toOld: OldRandomForestModel = null

  @Since("2.0.0")
  override def write: MLWriter = null
}

@Since("2.0.0")
object DecisionTreeBucketModel extends MLReadable[DecisionTreeBucketModel] {

  @Since("2.0.0")
  override def read: MLReader[DecisionTreeBucketModel] = null

  @Since("2.0.0")
  override def load(path: String): DecisionTreeBucketModel = null

  private[DecisionTreeBucketModel]
  class DecisionTreeBucketModelWriter(instance: DecisionTreeBucketModel)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {}
  }

  private class DecisionTreeBucketModelReader
    extends MLReader[DecisionTreeBucketModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[DecisionTreeBucketModel].getName
    private val treeClassName = classOf[DecisionTreeClassificationModel].getName

    override def load(path: String): DecisionTreeBucketModel = null
  }

  /** Convert a model from the old API */
  private[ml] def fromOld(
      oldModel: OldRandomForestModel,
      parent: DecisionTreeBucketizer,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int,
      numFeatures: Int = -1): DecisionTreeBucketModel = null
}
