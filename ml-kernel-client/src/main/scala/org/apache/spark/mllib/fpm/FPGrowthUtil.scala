// scalastyle:off
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.mllib.fpm

import org.apache.spark.Partitioner
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object FPGrowthUtils extends Logging {

  def genFreqItemsetsByOptLevel1[Item: ClassTag](
      data: RDD[Array[Item]],
      minCount: Long,
      freqItems: Array[Item],
      partitioner: Partitioner,
      timeLimit1: Double): RDD[FreqItemset[Item]] = {
    null
  }

  def genFreqItemsetsByOptLevel2[Item: ClassTag](
      data: RDD[Array[Item]],
      minCount: Long,
      freqItems: Array[Item],
      partitioner: Partitioner,
      timeLimit1: Double,
      timeLimit2: Double): RDD[FreqItemset[Item]] = {
    null
  }
}
