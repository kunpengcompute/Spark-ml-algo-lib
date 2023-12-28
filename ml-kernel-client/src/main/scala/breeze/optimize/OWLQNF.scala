// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package breeze.optimize

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.FirstOrderMinimizer.ConvergenceCheck
import breeze.util.SerializableLogging

class OWLQNF(convergenceCheck: ConvergenceCheck[BDV[Double]], m: Int, l1reg: Int => Double)
  extends OWLQN[Int, BDV[Double]](convergenceCheck, m, l1reg) with SerializableLogging {
  def this(maxIter: Int, m: Int, l1reg: Int => Double, tolerance: Double) =
    this(OWLQNF.checkConvergence(maxIter, tolerance), m, l1reg)
}

object OWLQNF {
  def checkConvergence(maxIter: Int, tolerance: Double): ConvergenceCheck[BDV[Double]] = {
    null
  }
}
