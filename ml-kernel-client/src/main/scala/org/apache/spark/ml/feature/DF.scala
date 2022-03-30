// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.feature

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

final case class DFParams(
    var dataPath: String = "",
    var splitMinSizeMB: Long = 1024L,
    var splitMaxSizeMB: Long = 2048L,
    var splitLocations: Long = 401L,
    var langFile: String = "languages.csv",
    var globalMerge: Boolean = false,
    var outputSeparator: String = "\t"
)

object DF {
  def compute(sc: SparkContext, params: DFParams): RDD[String] = null
}
