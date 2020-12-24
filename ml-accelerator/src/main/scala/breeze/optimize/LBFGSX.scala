// scalastyle:off header.matches
package breeze.optimize

/*
 Copyright 2009 David Hall, Daniel Ramage

 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

import breeze.linalg._
import breeze.math.MutableInnerProductModule
import breeze.optimize.FirstOrderMinimizerX.ConvergenceCheck
import breeze.util.SerializableLogging

/**
 * Port of LBFGS to Scala.
 *
 * Special note for LBFGS:
 *  If you use it in published work, you must cite one of:
 *     * J. Nocedal. Updating  Quasi-Newton  Matrices  with  Limited  Storage
 *    (1980), Mathematics of Computation 35, pp. 773-782.
 *  * D.C. Liu and J. Nocedal. On the  Limited  mem  Method  for  Large
 *    Scale  Optimization  (1989),  Mathematical  Programming  B,  45,  3,
 *    pp. 503-528.
 *
 * @param m: The memory of the search. 3 to 7 is usually sufficient.
 */
class LBFGSX[T](convergenceCheck: ConvergenceCheck[T], m: Int)
               (implicit space: MutableInnerProductModule[T, Double]) extends
  FirstOrderMinimizerX[T, DiffFunction[T]](convergenceCheck) with SerializableLogging {

  def this(maxIter: Int = -1, m: Int = 7, tolerance: Double = 1E-9)
          (implicit space: MutableInnerProductModule[T, Double]) =
    this(FirstOrderMinimizerX.defaultConvergenceCheckX(maxIter, tolerance), m )
  import space._
  require(m > 0)

  type History = LBFGSX.ApproximateInverseHessianX[T]

  override protected def adjustFunction(f: DiffFunction[T]): DiffFunction[T] = f.cached

  def takeStep(state: State, dir: T, stepSize: Double): T = state.x + dir * stepSize
  protected def initialHistory(f: DiffFunction[T], x: T):
  History = new LBFGSX.ApproximateInverseHessianX(m)
  protected def chooseDescentDirection(state: State, fn: DiffFunction[T]): T = {
    state.history * state.grad
  }

  protected def updateHistory(newX: T, newGrad: T, newVal: Double,
                              f: DiffFunction[T], oldState: State): History = {
    oldState.history.updated(newX - oldState.x, newGrad -:- oldState.grad)
  }


  override def updateTheta(f: DiffFunction[T], state: State): (T, T) = {
    val adjustedFun = adjustFunction(f)
    val dir = chooseDescentDirection(state, adjustedFun)
    val currentMomentum = ACC
      .updateMomentum(state.momentum, dir, inertiaCoefficient, momentumUpdateCoefficient)(space)
    val stepSize = 1.0
    logger.info(f"Step Size: $stepSize%.4g")
    val x = takeStep(state, currentMomentum, stepSize)
    (x, currentMomentum)
  }
}

object LBFGSX {
  case class ApproximateInverseHessianX[T](m: Int,
    private[LBFGSX] val memStep: IndexedSeq[T] = IndexedSeq.empty,
    private[LBFGSX] val memGradDelta: IndexedSeq[T] = IndexedSeq.empty)
   (implicit space: MutableInnerProductModule[T, Double])
    extends NumericOps[ApproximateInverseHessianX[T]] {

    import space._

    def repr: ApproximateInverseHessianX[T] = this

    def updated(step: T, gradDelta: T): ApproximateInverseHessianX[T] = {
      val (a, b) = ACC.update(step, gradDelta, this.memStep, this.memGradDelta, m)(space)
      new ApproximateInverseHessianX(m, a, b)
    }


    def historyLength: Int = memStep.length

    def *(grad: T): T = {
      val a = ACC.getInverseOfHessian(grad, this.memStep, this.memGradDelta, m, historyLength)
      a
    }
  }

}

