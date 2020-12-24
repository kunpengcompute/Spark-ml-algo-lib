// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package breeze.optimize

import breeze.math.MutableInnerProductModule

object ACC{

  def update[T](
     step: T,
     delta: T,
     mStep: IndexedSeq[T],
     g: IndexedSeq[T],
     m: Int) (implicit space: MutableInnerProductModule[T, Double]):
       (IndexedSeq[T], IndexedSeq[T]) = {
    null
  }

  def updateMomentum[T](
      m: T,
      dir: T,
      coeff: Double,
      uCoeff: Double) (implicit space: MutableInnerProductModule[T, Double]): T = {
    null.asInstanceOf[T]
  }

  def getInverseOfHessian[T](
      g: T,
      deltaA: IndexedSeq[T],
      deltaB: IndexedSeq[T],
      m: Int,
      size: Int)(implicit space: MutableInnerProductModule[T, Double]): T = {
    null.asInstanceOf[T]
  }
}
