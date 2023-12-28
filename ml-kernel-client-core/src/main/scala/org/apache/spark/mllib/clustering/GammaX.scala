// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.mllib.clustering

import org.apache.commons.math3.special.Gamma


object GammaX {
  def digamma(x: Double): Double = Gamma.digamma(x)
}
