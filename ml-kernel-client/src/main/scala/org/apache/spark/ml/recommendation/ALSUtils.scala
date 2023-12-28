// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
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
