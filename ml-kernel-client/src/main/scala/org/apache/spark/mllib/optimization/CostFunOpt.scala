// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.mllib.optimization

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object CostFunOpt {
  def aggregate(
      data: RDD[(Double, Vector)],
      dim: Int,
      gradient: Gradient,
      bcW: Broadcast[Vector]): (Vector, Double) = {
    null
  }
}
