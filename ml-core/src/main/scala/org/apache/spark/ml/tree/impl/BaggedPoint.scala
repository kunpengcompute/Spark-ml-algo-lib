/*
* Copyright (C) 2021-2022. Huawei Technologies Co., Ltd.
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

import org.apache.commons.math3.distribution.PoissonDistribution

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

/**
 * Internal representation of a datapoint which belongs to several subsamples of the same dataset,
 * particularly for bagging (e.g., for random forests).
 *
 * This holds one instance, as well as an array of weights which represent the (weighted)
 * number of times which this instance appears in each subsamplingRate.
 * E.g., (datum, [1, 0, 4]) indicates that there are 3 subsamples of the dataset and that
 * this datum has 1 copy, 0 copies, and 4 copies in the 3 subsamples, respectively.
 *
 * @param datum  Data instance
 * @param subsampleWeights  Weight of this instance in each subsampled dataset.
 * @param sampleId ID of sample
 *
 * TODO: This does not currently support (Double) weighted instances.  Once MLlib has weighted
 *       dataset support, update.  (We store subsampleWeights as Double for this future extension.)
 */
private[spark] class BaggedPoint[Datum](
    val datum: Datum,
    val subsampleWeights: Array[Int],
    var sampleId: Short = 0)
  extends Serializable

private[spark] object BaggedPoint {

  /**
   * Convert an input dataset into its BaggedPoint representation,
   * choosing subsamplingRate counts for each instance.
   * Each subsamplingRate has the same number of instances as the original dataset,
   * and is created by subsampling without replacement.
   * @param input Input dataset.
   * @param subsamplingRate Fraction of the training data used for learning decision tree.
   * @param numSubsamples Number of subsamples of this RDD to take.
   * @param withReplacement Sampling with/without replacement.
   * @param seed Random seed.
   * @return BaggedPoint dataset representation.
   */
  def convertToBaggedRDD[Datum] (
      input: RDD[Datum],
      subsamplingRate: Double,
      numSubsamples: Int,
      withReplacement: Boolean,
      seed: Long = Utils.random.nextLong(),
      oneFeaturePerTree: Boolean = false): RDD[BaggedPoint[Datum]] = {
    if (oneFeaturePerTree) {
      convertToBaggedRDDWithoutSampling(input)
    } else {
      if (withReplacement) {
        convertToBaggedRDDSamplingWithReplacement(input, subsamplingRate, numSubsamples, seed)
      } else {
        if (numSubsamples == 1 && subsamplingRate == 1.0) {
          convertToBaggedRDDWithoutSampling(input)
        } else {
          convertToBaggedRDDSamplingWithoutReplacement(input, subsamplingRate, numSubsamples, seed)
        }
      }
    }
  }

  private def convertToBaggedRDDSamplingWithoutReplacement[Datum] (
      input: RDD[Datum],
      subsamplingRate: Double,
      numSubsamples: Int,
      seed: Long): RDD[BaggedPoint[Datum]] = {
    input.mapPartitionsWithIndex { (partitionIndex, instances) =>
      // Use random seed = seed + partitionIndex + 1 to make generation reproducible.
      val rng = new XORShiftRandom
      rng.setSeed(seed + partitionIndex + 1)
      instances.map { instance =>
        val subsampleWeights = new Array[Int](numSubsamples)
        var subsampleIndex = 0
        while (subsampleIndex < numSubsamples) {
          val x = rng.nextDouble()
          subsampleWeights(subsampleIndex) = {
            if (x < subsamplingRate) 1 else 0
          }
          subsampleIndex += 1
        }
        new BaggedPoint(instance, subsampleWeights)
      }
    }
  }

  private def convertToBaggedRDDSamplingWithReplacement[Datum] (
      input: RDD[Datum],
      subsample: Double,
      numSubsamples: Int,
      seed: Long): RDD[BaggedPoint[Datum]] = {
    input.mapPartitionsWithIndex { (partitionIndex, instances) =>
      // Use random seed = seed + partitionIndex + 1 to make generation reproducible.
      val poisson = new PoissonDistribution(subsample)
      poisson.reseedRandomGenerator(seed + partitionIndex + 1)
      instances.map { instance =>
        val subsampleWeights = new Array[Int](numSubsamples)
        var subsampleIndex = 0
        while (subsampleIndex < numSubsamples) {
          subsampleWeights(subsampleIndex) = poisson.sample()
          subsampleIndex += 1
        }
        new BaggedPoint(instance, subsampleWeights)
      }
    }
  }

  private def convertToBaggedRDDWithoutSampling[Datum] (
      input: RDD[Datum]): RDD[BaggedPoint[Datum]] = {
    input.map(datum => new BaggedPoint(datum, Array(1)))
  }

}
