package breeze.optimize

import breeze.math.MutableInnerProductModule

object ACC{

  def update[T](
     step: T,
     delta: T,
     mStep: IndexedSeq[T],
     g: IndexedSeq[T],
     m: Int) (implicit space: MutableInnerProductModule[T, Double]): (IndexedSeq[T], IndexedSeq[T]) = {
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
