// scalastyle:off header.matches
/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
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

package org.apache.spark.mllib.stat.correlation

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object SpearmanCorrelationUtil {

  /**
   * Get a matrix of ranks for the matrix given
   */
  def getRanks(X: RDD[Vector]): RDD[Vector] = {
    null
  }

}
