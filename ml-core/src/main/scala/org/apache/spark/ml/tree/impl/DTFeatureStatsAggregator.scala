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

import org.apache.spark.mllib.tree.impurity._


/**
 * DecisionTree statistics aggregator for a feature for a node.
 * This class is abstract to support learning with and without feature subsampling.
 */
private[spark] class DTFeatureStatsAggregator(
    val metadata: DecisionTreeMetadata,
    val _featureIndex: Int) extends Serializable {

  /**
   * [[ImpurityAggregator]] instance specifying the impurity type.
   */

  val impurityAggregator = new VarianceAggregator()

  val featureIndex: Int = _featureIndex

  /**
   * Number of elements (Double values) used for the sufficient statistics of each bin.
   */
  private val statsSize: Int = impurityAggregator.statsSize

  /**
   * Number of bins for the feature.
   */
  private val numBins: Int = {
      metadata.numBins(featureIndex)
  }

  /**
   * Total number of elements stored in this aggregator
   */
  private val allStatsSize: Int = numBins * statsSize

  /**
   * Flat array of elements.
   */
  private val allStats: Array[Double] = new Array[Double](allStatsSize)

  /**
   * Array of parent node sufficient stats.
   */
  private val parentStats: Array[Double] = new Array[Double](statsSize)

  /**
   * Get an [[ImpurityCalculator]] for a given (node, feature, bin).
   */
  def getImpurityCalculator(featureOffset: Int, binIndex: Int): ImpurityCalculator = {
    impurityAggregator.getCalculator(allStats, binIndex * statsSize)
  }

  /**
   * Get an [[ImpurityCalculator]] for the parent node.
   */
  def getParentImpurityCalculator(): ImpurityCalculator = {
    impurityAggregator.getCalculator(parentStats, 0)
  }

  /**
   * Update the stats for a given bin for ordered features, using the given label.
   */
  def updateX(featureIndex: Int, binIndex: Int, label: Double): Unit = {
    val i = binIndex * statsSize
    impurityAggregator.updateX(allStats, i, label)
  }

  /**
   * Pre-compute feature offset for use with [[featureUpdate]].
   * For ordered features only.
   */
  def getFeatureOffset(featureIndex: Int): Int = 0

  /**
   * For a given feature, merge the stats for two bins.
   *
   * @param featureOffset  This is a pre-computed feature offset
   *                           from [[getFeatureOffset]].
   * @param binIndex  The other bin is merged into this bin.
   * @param otherBinIndex  This bin is not modified.  X
   */
  def mergeForFeature(featureOffset: Int, binIndex: Int, otherBinIndex: Int): Unit = {
    impurityAggregator.merge(allStats, binIndex * statsSize, otherBinIndex * statsSize)
  }
}
