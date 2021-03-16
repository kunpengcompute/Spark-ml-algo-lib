// scalastyle:off header.matches
/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
/*
 * Copyright (c). Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package org.apache.spark.ml.recommendation

import org.apache.spark.ml.recommendation.ALS.ALSPartitioner
import org.apache.spark.rdd.RDD

object ALSUtils {
  type OutBlock = Array[Array[Int]]

  def mergeBlock(in: RDD[(Int, OutBlock)], pl: Int): RDD[(Int, Array[(Int, Array[Int])])] = {
    null
  }
}
