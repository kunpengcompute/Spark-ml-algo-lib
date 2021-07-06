// scalastyle:off header.matches
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

package org.apache.spark.mllib.stat.correlation

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Compute Spearman's correlation for two RDDs of the type RDD[Double] or the correlation matrix
 * for an RDD of the type RDD[Vector].
 *
 * Definition of Spearman's correlation can be found at
 * http://en.wikipedia.org/wiki/Spearman's_rank_correlation_coefficient
 */
private[stat] object SpearmanCorrelation extends Correlation with Logging {

  /**
   * Compute Spearman's correlation for two datasets.
   */
  override def computeCorrelation(x: RDD[Double], y: RDD[Double]): Double = {
    computeCorrelationWithMatrixImpl(x, y)
  }

  /**
   * Compute Spearman's correlation matrix S, for the input matrix, where S(i, j) is the
   * correlation between column i and j.
   */
  override def computeCorrelationMatrix(X: RDD[Vector]): Matrix = {
    val groupedRanks = SpearmanCorrelationUtil.getRanks(X)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .setName("groupedRanks")
    groupedRanks.foreach(_ => {})
    X.sparkContext.getPersistentRDDs.foreach{case (_, rdd) =>
      if (!rdd.name.contains("groupedRanks")) rdd.unpersist()
    }
    val rowMatrix = new RowMatrix(groupedRanks)
    val cov = rowMatrix.computeCovariance()
    PearsonCorrelation.computeCorrelationMatrixFromCovariance(cov)
  }
}
