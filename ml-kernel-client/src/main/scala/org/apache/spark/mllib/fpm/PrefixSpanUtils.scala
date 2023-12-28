// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.mllib.fpm

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

object PrefixSpanUtils extends Logging {
  def computePatterns(data: RDD[Array[Int]],
      minCount: Long,
      maxPatternLength: Int,
      maxLocalProjDBSize: Long): RDD[(Array[Int], Long)] = {
    null
  }

}
