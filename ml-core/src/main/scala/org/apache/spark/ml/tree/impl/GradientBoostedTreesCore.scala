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

import it.unimi.dsi.fastutil.objects.ObjectArrayList

import org.apache.spark.internal.Logging
import org.apache.spark.ml.tree.{CategoricalSplit, ContinuousSplit, LearningNode, Split}
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.ImpurityStats

object GradientBoostedTreesCore extends Logging{
  private[tree] class NodeIndexInfo(
      val nodeIndexInGroup: Int,
      val featureSubset: Option[Array[Int]],
      val featureSubsetHashSetX: Option[scala.collection.mutable.HashSet[Int]] = None)
    extends Serializable

  /**
   * Calculate the impurity statistics for a given (feature, split) based upon left/right
   * aggregates.
   *
   * @param stats the recycle impurity statistics for this feature's all splits,
   *              only 'impurity' and 'impurityCalculator' are valid between each iteration
   * @param leftImpurityCalculator left node aggregates for this (feature, split)
   * @param rightImpurityCalculator right node aggregate for this (feature, split)
   * @param metadata learning and dataset metadata for DecisionTree
   * @return Impurity statistics for this (feature, split)
   */
  private def calculateImpurityStats(
      stats: ImpurityStats,
      leftImpurityCalculator: ImpurityCalculator,
      rightImpurityCalculator: ImpurityCalculator,
      metadata: DecisionTreeMetadata): ImpurityStats = {

    val parentImpurityCalculator: ImpurityCalculator = if (stats == null) {
      leftImpurityCalculator.copy.add(rightImpurityCalculator)
    } else {
      stats.impurityCalculator
    }

    val impurity: Double = if (stats == null) {
      parentImpurityCalculator.calculate()
    } else {
      stats.impurity
    }

    val leftCount = leftImpurityCalculator.count
    val rightCount = rightImpurityCalculator.count

    val totalCount = leftCount + rightCount

    // If left child or right child doesn't satisfy minimum instances per node,
    // then this split is invalid, return invalid information gain stats.
    if ((leftCount < metadata.minInstancesPerNode) ||
      (rightCount < metadata.minInstancesPerNode)) {
      return ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator)
    }

    val leftImpurity = leftImpurityCalculator.calculate() // Note: This equals 0 if count = 0
    val rightImpurity = rightImpurityCalculator.calculate()

    val leftWeight = leftCount / totalCount.toDouble
    val rightWeight = rightCount / totalCount.toDouble

    val gain = impurity - leftWeight * leftImpurity - rightWeight * rightImpurity

    // if information gain doesn't satisfy minimum information gain,
    // then this split is invalid, return invalid information gain stats.
    if (gain < metadata.minInfoGain) {
      return ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator)
    }

    new ImpurityStats(gain, impurity, parentImpurityCalculator,
      leftImpurityCalculator, rightImpurityCalculator)
  }

  /**
   * Find the best split for a node.
   *
   * @param binAggregates Bin statistics.
   * @return tuple for best split: (Split, information gain, prediction at node)
   */
  private[tree] def binsToBestSplitX(
      binAggregates: DTFeatureStatsAggregator,
      splits: ObjectArrayList[Split],
      featureIndex: Int,
      node: LearningNode): (Split, ImpurityStats) = {

    // Calculate InformationGain and ImpurityStats if current node is top node
    val level = LearningNode.indexToLevel(node.id)
    var gainAndImpurityStats: ImpurityStats = if (level == 0) {
      null
    } else {
      node.stats
    }

    if (binAggregates.metadata.numSplits(featureIndex) != 0) {
      val featureIndexIdx = featureIndex
      val numSplits = binAggregates.metadata.numSplits(featureIndex)
      if (binAggregates.metadata.isContinuous(featureIndex)) {
        // Cumulative sum (scanLeft) of bin statistics.
        // Afterwards, binAggregates for a bin is the sum of aggregates for
        // that bin + all preceding bins.
        var splitIndex = 0
        while (splitIndex < numSplits) {
          binAggregates.mergeForFeature(0, splitIndex + 1, splitIndex)
          splitIndex += 1
        }
        // Find best split.
        val (bestFeatureSplitIndex, bestFeatureGainStats) =
          Range(0, numSplits).map { case splitIdx =>
            val leftChildStats = binAggregates.getImpurityCalculator(0, splitIdx)
            val rightChildStats =
              binAggregates.getImpurityCalculator(0, numSplits)
            rightChildStats.subtract(leftChildStats)
            gainAndImpurityStats = calculateImpurityStats(gainAndImpurityStats,
              leftChildStats, rightChildStats, binAggregates.metadata)
            (splitIdx, gainAndImpurityStats)
          }.maxBy(_._2.gain)
        (splits.get(bestFeatureSplitIndex), bestFeatureGainStats)
      } else if (binAggregates.metadata.isUnordered(featureIndex)) {
        // unreachable for GBDT
        // Unordered categorical feature
        // val leftChildOffset = binAggregates.getFeatureOffset(featureIndexIdx)
        val (bestFeatureSplitIndex, bestFeatureGainStats) =
          Range(0, numSplits).map { splitIndex =>
            val leftChildStats = binAggregates.getImpurityCalculator(0, splitIndex)
            val rightChildStats = binAggregates.getImpurityCalculator(0, numSplits)
              .subtract(leftChildStats)
            gainAndImpurityStats = calculateImpurityStats(gainAndImpurityStats,
              leftChildStats, rightChildStats, binAggregates.metadata)
            (splitIndex, gainAndImpurityStats)
          }.maxBy(_._2.gain)
        (splits.get(bestFeatureSplitIndex), bestFeatureGainStats)
      } else {
        // Ordered categorical feature
        val numCategories = binAggregates.metadata.numBins(featureIndex)

        /* Each bin is one category (feature value).
         * The bins are ordered based on centroidForCategories, and this ordering determines which
         * splits are considered.  (With K categories, we consider K - 1 possible splits.)
         *
         * centroidForCategories is a list: (category, centroid)
         */
        val centroidForCategories = Range(0, numCategories).map { case featureValue =>
          val categoryStats =
            binAggregates.getImpurityCalculator(0, featureValue)
          val centroid = if (categoryStats.count != 0) {
            if (binAggregates.metadata.isMulticlass) {
              // unreachable for GBDT
              // multiclass classification
              // For categorical variables in multiclass classification,
              // the bins are ordered by the impurity of their corresponding labels.
              categoryStats.calculate()
            } else if (binAggregates.metadata.isClassification) {
              // unreachable for GBDT
              // binary classification
              // For categorical variables in binary classification,
              // the bins are ordered by the count of class 1.
              categoryStats.stats(1)
            } else {
              // regression
              // For categorical variables in regression and binary classification,
              // the bins are ordered by the prediction.
              categoryStats.predict
            }
          } else {
            Double.MaxValue
          }
          (featureValue, centroid)
        }

        logDebug(s"Centroids for categorical variable: ${centroidForCategories.mkString(",")}")

        // bins sorted by centroids
        val categoriesSortedByCentroid = centroidForCategories.toList.sortBy(_._2)

        logDebug("Sorted centroids for categorical variable = " +
          categoriesSortedByCentroid.mkString(","))

        // Cumulative sum (scanLeft) of bin statistics.
        // Afterwards, binAggregates for a bin is the sum of aggregates for
        // that bin + all preceding bins.
        var splitIndex = 0
        while (splitIndex < numSplits) {
          val currentCategory = categoriesSortedByCentroid(splitIndex)._1
          val nextCategory = categoriesSortedByCentroid(splitIndex + 1)._1
          binAggregates.mergeForFeature(0, nextCategory, currentCategory)
          splitIndex += 1
        }
        // lastCategory = index of bin with total aggregates for this (node, feature)
        val lastCategory = categoriesSortedByCentroid.last._1
        // Find best split.
        val (bestFeatureSplitIndex, bestFeatureGainStats) =
          Range(0, numSplits).map { splitIndex =>
            val featureValue = categoriesSortedByCentroid(splitIndex)._1
            val leftChildStats =
              binAggregates.getImpurityCalculator(0, featureValue)
            val rightChildStats =
              binAggregates.getImpurityCalculator(0, lastCategory)
            rightChildStats.subtract(leftChildStats)
            gainAndImpurityStats = calculateImpurityStats(gainAndImpurityStats,
              leftChildStats, rightChildStats, binAggregates.metadata)
            (splitIndex, gainAndImpurityStats)
          }.maxBy(_._2.gain)
        val categoriesForSplit =
          categoriesSortedByCentroid.map(_._1.toDouble).slice(0, bestFeatureSplitIndex + 1)
        val bestFeatureSplit =
          new CategoricalSplit(featureIndex, categoriesForSplit.toArray, numCategories)
        (bestFeatureSplit, bestFeatureGainStats)
      }
    } else {
      // If no valid splits for features, then this split is invalid,
      // return invalid information gain stats.  Take any split and continue.
      // Splits is empty, so arbitrarily choose to split on any threshold
      // val parentImpurityCalculator = binAggregates.getParentImpurityCalculator()
      // No split, no need to merge
      val featureIndexIdx = featureIndex
      val numSplits = binAggregates.metadata.numSplits(featureIndex)
      val parentImpurityCalculator = binAggregates.getImpurityCalculator(0, numSplits)
      if (binAggregates.metadata.isContinuous(featureIndex)) {
        (new ContinuousSplit(featureIndex, 0),
          ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator))
      } else {
        // Seems like unreachable for GBDT (as well as RF)
        val numCategories = binAggregates.metadata.featureArity(featureIndex)
        (new CategoricalSplit(featureIndex, Array(), numCategories),
          ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator))
      }
    }

    // For each (feature, split), calculate the gain, and select the best (feature, split).
  }
}
