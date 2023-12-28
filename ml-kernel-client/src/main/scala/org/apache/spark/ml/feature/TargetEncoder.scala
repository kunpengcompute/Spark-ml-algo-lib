// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.ml.feature

import org.apache.spark.ml.{Estimator, Model, PipelineStage}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable,
  MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

trait TargetEncoderParams extends Params {
  /**
   * Get the name of the fold column for out-of-fold encoding.
   * @return name of the fold column
   */
  def getFoldCol: String = null

  /**
   * Get the name of the label column.
   * @return name of the label column
   */
  def getLabelCol: String = null

  /**
   * Get names of the columns to encode.
   * @return names of the columns to encode
   */
  def getInputCols: Array[String] = null

  /**
   * Get number of folds for out-of-fold encoding.
   * @return number of unique fold ids in the fold column
   */
  def getNumFolds: Int = 0

  /**
   * Get the seed to generate fold column if the fold column is not provided.
   * @return seed to generate fold column
   */
  def getFoldSeed: Int = 0

  /**
   * Get names of the output columns. If not set, they will be the input column names appended with
   * a "_te" suffix.
   * @return names of the output columns
   */
  def getOutputCols: Array[String] = null

  /**
   * Get the BlendedAvgInflectionPoint parameter.
   * @return the BlendedAvgInflectionPoint parameter
   */
  def getBlendedAvgInflectionPoint: Double = 0

  /**
   * Get the BlendedAvgSmoothing parameter.
   * @return the BlendedAvgSmoothing parameter
   */
  def getBlendedAvgSmoothing: Double = 0
}

trait TargetEncoderBase extends PipelineStage with TargetEncoderParams {
  override def transformSchema(schema: StructType): StructType = null
}

/**
 * TargetEncoder is a feature engineering method for categorical features. It replaces each category
 * with its target mean, transforming columns of categorical values into columns of Double vectors.
 * It supports both classification and regression problems. For classification problems, the length
 * of result vectors equals to number of classes - 1. For regression, the length of result vectors
 * equals to 1. The implementation handles target leakage by out-of-fold encoding and blended
 * averaging.
 * @param uid uid of TargetEncoder
 */
class TargetEncoder(override val uid: String)
  extends Estimator[TargetEncoderModel]
  with TargetEncoderParams with TargetEncoderBase
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("TargetEncoder"))

  /**
   * Fit the given dataset with TargetEncoder.
   * @param dataset dataset to be fitted
   * @return TargetEncoderModel
   */
  override def fit(dataset: Dataset[_]): TargetEncoderModel = null

  /**
   * Copy a TargetEncoder instance.
   * @param extra extra ParamMap
   * @return TargetEncoder
   */
  override def copy(extra: ParamMap): TargetEncoder = null

  /**
   * Set the name of the label column. The label column should be Double type and contains no
   * missing value. For classification problem, the value of label column should be in range
   * [0, numClasses - 1].
   * @param value name of the label column
   * @return TargetEncoder
   */
  def setLabelCol(value: String): this.type = null

  /**
   * Set names of the columns to encode. Input columns should be String type. Missing values are
   * treated as a new category, handled automatically by TargetEncoder.
   * @param value names of the columns to encode
   * @return TargetEncoder
   */
  def setInputCols(value: Array[String]): this.type = null

  /**
   * Set names of the output columns. The order of output columns should match input columns. If
   * not set, they will be input column names appended with a "_te" suffix.
   * @param value names of the output columns
   * @return TargetEncoder
   */
  def setOutputCols(value: Array[String]): this.type = null

  /**
   * Set number of folds for out-of-fold encoding. Larger value means stronger regularization. If
   * fold column is provided in the training DataFrame, numFolds should match the number of unique
   * fold ids in it. When not provided, a fold column will be automatically created according to
   * this value. NumFolds should be in range [2, 10], the default value is 4.
   * @param value number of unique fold ids in the fold column
   * @return TargetEncoder
   */
  def setNumFolds(value: Int): this.type = null

  /**
   * Set name of the fold column for out-of-fold encoding. The fold column is an Int column with
   * range [0, numFolds - 1], a category will be encoded with all targets of the same category from
   * different fold ids. If the column is not provided in training DataFrame, it will be
   * automatically created. The default value is "fold".
   * @param value number of unique fold ids in the fold column
   * @return TargetEncoder
   */
  def setFoldCol(value: String): this.type = null

  /**
   * Set the seed for generating the fold column when it is not provided in the training DataFrame.
   * @param value seed for generating the fold column
   * @return TargetEncoder
   */
  def setFoldSeed(value: Int): this.type = null

  /**
   * Set the parameter of the blended average. The bigger number is set, the groups relatively
   * bigger to the overall data set size will consider the global target value as a component in the
   * weighted average. The default value is 10.
   * @param value parameter of the blended average
   * @return TargetEncoder
   */
  def setBlendedAvgInflectionPoint(value: Double): this.type = null

  /**
   * Set the parameter of blended average. Controls the rate of transition between a group target
   * value and a global target value. The default value is 20. It should be positive.
   * @param value parameter of the blended average
   * @return TargetEncoder
   */
  def setBlendedAvgSmoothing(value: Double): this.type = null

  /**
   * Set the problem type. Supported problem types include "classification" and "regression".
   * @param value problem type
   * @return TargetEncoder
   */
  def setProblemType(value: String): this.type = null
}

/**
 * TargetEncoderModel contains the aggregation information of the training DataFrame.
 * @param uid uid of TargetEncoderModel
 * @param ooFoldAggs out-of-fold aggregation
 * @param globalAggs global aggregation
 * @param prior target mean
 */
class TargetEncoderModel(
    override val uid: String,
    val ooFoldAggs: Array[RDD[(Long, (Array[Double], Int))]],
    val globalAggs: Array[RDD[(Long, (Array[Double], Int))]],
    val prior: Array[Double])
  extends Model[TargetEncoderModel] with MLWritable with DefaultParamsWritable
  with TargetEncoderBase with TargetEncoderParams {
  /**
   * Transform the dataset with target encoding. Out-of-fold encoding is only relevant for training
   * datasets. Thus the transform method has to treat training dataset and test dataset differently.
   * When TargetEncoder is used inside a ML pipeline, the differentiation is done automatically. But
   * if a user decides to use TargetEncoder without ML pipeline, use "transformTrainingDataset" and
   * "transformTestDataset" instead.
   * @param dataset dataset to be transformed
   * @return transformed dataset
   */
  override def transform(dataset: Dataset[_]): DataFrame = null

  /**
   * Transform the training dataset with out-of-fold encoding to prevent target leakage. Unseen
   * categories will be encoded with the target mean of the whole training dataset.
   * @param dataset training dataset
   * @return transformed training dataset
   */
  def transformTrainingDataset(dataset: Dataset[_]): DataFrame = null

  /**
   * Encode the test dataset with global category mean.
   * @param dataset test dataset
   * @return transformed test dataset
   */
  def transformTestDataset(dataset: Dataset[_]): DataFrame = null

  /**
   * Copy a TargetEncoderModel instance.
   * @param extra extra ParamMap
   * @return TargetEncoderModel
   */
  override def copy(extra: ParamMap): TargetEncoderModel = null

  /**
   * Write method.
   * @return MLWriter
   */
  override def write(): MLWriter = null
}

object TargetEncoderModel extends MLReadable[TargetEncoderModel] {
  /**
   * Read method.
   * @return MLReader[TargetEncoderModel]
   */
  override def read: MLReader[TargetEncoderModel] = null
}
