// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package breeze.optimize

import breeze.linalg.DenseVector
import breeze.optimize.FirstOrderMinimizer.ConvergenceCheck
import breeze.util.SerializableLogging

class LBFGSN(convergenceCheck: ConvergenceCheck[DenseVector[Double]], m: Int)
  extends LBFGS[DenseVector[Double]](convergenceCheck, m) with SerializableLogging {
  def this(
      maxIter: Int,
      m: Int,
      tolerance: Double,
      absoluteConvergenceCheck: Boolean = true,
      fValMemory: Int = 2) = {
    this(LBFGSN.checkConvergence(maxIter, tolerance, absoluteConvergenceCheck, fValMemory), m)
  }
}

object LBFGSN {
  def checkConvergence(
      maxIter: Int,
      tol: Double,
      absoluteConvergenceCheck: Boolean,
      fValMemory: Int): ConvergenceCheck[DenseVector[Double]] = {
    null
  }
}
