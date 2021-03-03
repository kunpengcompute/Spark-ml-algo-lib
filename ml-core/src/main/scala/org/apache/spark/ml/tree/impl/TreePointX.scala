/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
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

package org.apache.spark.ml.tree.impl

import it.unimi.dsi.fastutil.ints.Int2CharOpenHashMap

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.tree.{ContinuousSplit, Split}
import org.apache.spark.ml.tree.impl.BinnedFeaturesDataType.BinnedFeaturesDataType
import org.apache.spark.rdd.RDD


/**
 * Enum for selecting the data type of binned features of training samples
 */
object BinnedFeaturesDataType extends Enumeration {
  type BinnedFeaturesDataType = Value
  val array, fasthashmap = Value
}


/**
 * Internal representation of LabeledPoint for DecisionTree.
 * This bins feature values based on a subsampled of data as follows:
 *  (a) Continuous features are binned into ranges.
 *  (b) Unordered categorical features are binned based on subsets of feature values.
 *      "Unordered categorical features" are categorical features with low arity used in
 *      multiclass classification.
 *  (c) Ordered categorical features are binned based on feature values.
 *      "Ordered categorical features" are categorical features with high arity,
 *      or any categorical feature used in regression or binary classification.
 *
 * @param label  Label from LabeledPoint
 * @param binnedFeatures  Binned feature values.
 *                        Same length as LabeledPoint.features, but values are bin indices.
 */
private[spark] class TreePointX(val label: Double, val binnedFeatures: BinnedFeature)
  extends Serializable {
}

private[spark] object TreePointX {

  /**
   * Convert an input dataset into its TreePointX representation,
   * binning feature values in preparation for DecisionTree training.
   * @param input     Input dataset.
   * @param splits    Splits for features, of size (numFeatures, numSplits).
   * @param metadata  Learning and dataset metadata
   * @return  TreePointX dataset representation
   */
  def convertToTreeRDD(
      input: RDD[LabeledPoint],
      splits: Array[Array[Split]],
      metadata: DecisionTreeMetadata): RDD[TreePointX] = {
    convertToTreeRDD(input, splits, metadata, BinnedFeaturesDataType.array)
  }

  /**
   * Convert an input dataset into its TreePointX representation,
   * binning feature values in preparation for DecisionTree training.
   * @param input     Input dataset.
   * @param splits    Splits for features, of size (numFeatures, numSplits).
   * @param metadata  Learning and dataset metadata
   * @return  TreePointX dataset representation
   */
  def convertToTreeRDD(
      input: RDD[LabeledPoint],
      splits: Array[Array[Split]],
      metadata: DecisionTreeMetadata,
      binnedFeaturesType: BinnedFeaturesDataType): RDD[TreePointX] = {
    // Construct arrays for featureArity for efficiency in the inner loop.
    val featureArity: Array[Int] = new Array[Int](metadata.numFeatures)
    var featureIndex = 0
    while (featureIndex < metadata.numFeatures) {
      featureArity(featureIndex) = metadata.featureArity.getOrElse(featureIndex, 0)
      featureIndex += 1
    }
    val thresholds: Array[Array[Double]] = featureArity.zipWithIndex.map { case (arity, idx) =>
      if (arity == 0) {
        splits(idx).map(_.asInstanceOf[ContinuousSplit].threshold)
      } else {
        Array.empty[Double]
      }
    }
    val useArrayType = (binnedFeaturesType == BinnedFeaturesDataType.array)
    if (useArrayType) {
      input.map { x =>
        TreePointX.labeledPointToTreePointByArray(x, thresholds, featureArity)
      }
    } else {
      input.map { x =>
        TreePointX.labeledPointToTreePointByFastHashMap(x, thresholds, featureArity)
      }
    }
  }

  /**
   * Convert one LabeledPoint into its TreePointX representation.
   * @param thresholds  For each feature, split thresholds for continuous features,
   *                    empty for categorical features.
   * @param featureArity  Array indexed by feature, with value 0 for continuous and numCategories
   *                      for categorical features.
   */
  private[spark] def labeledPointToTreePointByArray(
      labeledPoint: LabeledPoint,
      thresholds: Array[Array[Double]],
      featureArity: Array[Int]): TreePointX = {
    val numFeatures = labeledPoint.features.size
    val arr = new Array[Char](numFeatures)
    var featureIndex = 0
    while (featureIndex < numFeatures) {
      arr(featureIndex) =
        findBin(featureIndex, labeledPoint, featureArity(featureIndex), thresholds(featureIndex))
          .toChar
      featureIndex += 1
    }
    new TreePointX(labeledPoint.label, new BinnedFeatureArray(arr))
  }

  /**
   * Convert one LabeledPoint into its TreePointX representation.
   * @param thresholds  For each feature, split thresholds for continuous features,
   *                    empty for categorical features.
   * @param featureArity  Array indexed by feature, with value 0 for continuous and numCategories
   *                      for categorical features.
   */
  private[spark] def labeledPointToTreePointByFastHashMap(
      labeledPoint: LabeledPoint,
      thresholds: Array[Array[Double]],
      featureArity: Array[Int]): TreePointX = {
    val numFeatures = labeledPoint.features.size
    val binFeaturesMap = new Int2CharOpenHashMap()
    var featureIndex = 0
    while (featureIndex < numFeatures) {
      val binFeature =
        findBin(featureIndex, labeledPoint, featureArity(featureIndex), thresholds(featureIndex))
          .toChar
      if (binFeature != '\u0000') {
        binFeaturesMap.put(featureIndex, binFeature)
      }
      featureIndex += 1
    }
    val binFeatures = new BinnedFeatureFastHashMap(binFeaturesMap)
    new TreePointX(labeledPoint.label, binFeatures)
  }

  /**
   * Find discretized value for one (labeledPoint, feature).
   *
   * NOTE: We cannot use Bucketizer since it handles split thresholds differently than the old
   *       (mllib) tree API.  We want to maintain the same behavior as the old tree API.
   *
   * @param featureArity  0 for continuous features; number of categories for categorical features.
   */
  private def findBin(
      featureIndex: Int,
      labeledPoint: LabeledPoint,
      featureArity: Int,
      thresholds: Array[Double]): Int = {
    val featureValue = labeledPoint.features(featureIndex)

    if (featureArity == 0) {
      val idx = java.util.Arrays.binarySearch(thresholds, featureValue)
      if (idx >= 0) {
        idx
      } else {
        -idx - 1
      }
    } else {
      // Categorical feature bins are indexed by feature values.
      if (featureValue < 0 || featureValue >= featureArity) {
        throw new IllegalArgumentException(
          s"DecisionTree given invalid data:" +
            s" Feature $featureIndex is categorical with values in {0,...,${featureArity - 1}," +
            s" but a data point gives it value $featureValue.\n" +
            s"  Bad data point: ${labeledPoint.toString}")
      }
      featureValue.toInt
    }
  }
}

private[spark] class SparseFeatureArray(val indies: Array[Int], val binnedFeatures: Array[Char])
  extends Serializable {
  def get(index: Int): Char = {
    val i = java.util.Arrays.binarySearch(indies, index)
    if (i < 0) {
      '\u0000'
    } else {
      binnedFeatures(i)
    }
  }
}

private[spark] trait BinnedFeature {
  def get(index: Int): Char
}

private[spark] class BinnedFeatureArray(val features: Array[Char])
  extends Serializable with BinnedFeature{
  override def get(index: Int): Char = features(index)
}

private[spark] class BinnedFeatureFastHashMap(val featureMap: Int2CharOpenHashMap)
  extends Serializable with BinnedFeature {
  override def get(index: Int): Char = featureMap.get(index)
}

