/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package breeze.optimize

import java.util.Date

import FirstOrderMinimizerX.ConvergenceCheck
import breeze.linalg.norm
import breeze.math.{MutableEnumeratedCoordinateField, MutableFiniteCoordinateField, MutableInnerProductModule, NormedModule}
import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator}
import breeze.util.Implicits._
import breeze.util.SerializableLogging



/**
 *
 * @author dlwh
 */
abstract class FirstOrderMinimizerX[T, DF<:StochasticDiffFunction[T]]
(val convergenceCheck: ConvergenceCheck[T])
(implicit space: MutableInnerProductModule[T, Double])
  extends Minimizer[T, DF] with SerializableLogging {

  def this(maxIter: Int = -1, tolerance: Double = 1E-6,
           fvalMemory: Int = 100, relativeTolerance: Boolean = true)
          (implicit space: MutableInnerProductModule[T, Double]) =
    this(FirstOrderMinimizerX.defaultConvergenceCheckX[T]
      (maxIter, tolerance, relativeTolerance, fvalMemory))

  var inertiaCoefficient: Double = 0.5
  val momentumUpdateCoefficient : Double = 0.9

  def setInertiaCoefficient(a: Double): Unit = {
    this.inertiaCoefficient = a
  }

  /**
   * Any history the derived minimization function needs to do its updates.
   * typically an approximation
   * to the second derivative/hessian matrix.
   */
  type History
  type State = FirstOrderMinimizerX.State[T, convergenceCheck.Info, History]




  protected def initialHistory(f: DF, init: T): History
  protected def adjustFunction(f: DF): DF = f
  protected def adjust(newX: T, newGrad: T, newVal: Double): (Double, T) = (newVal, newGrad)
  protected def chooseDescentDirection(state: State, f: DF): T
  protected def takeStep(state: State, dir: T, stepSize:Double): T
  protected def updateHistory(newX: T, newGrad: T, newVal: Double, f: DF, oldState: State): History
  protected def updateTheta(f: DF, state: State): (T, T)


  protected def initialState(f: DF, init: T): State = {
    val x = init
    val history = initialHistory(f, init)
    val (value, grad) = calculateObjective(f, x, history)
    val (adjValue, adjGrad) = adjust(x, grad, value)
    import space._
    val copyInit = space.copy(init)
    copyInit -= copyInit
    FirstOrderMinimizerX.State(x,
      value,
      grad,
      adjValue,
      adjGrad,
      0,
      adjValue,
      history,
      convergenceCheck.initialInfo,
      copyInit)
  }


  protected def calculateObjective(f: DF, x: T, history: History): (Double, T) = {
    f.calculate(x)
  }

  def infiniteIterations(f: DF, state: State): Iterator[State] = {
    var failedOnce = false
    val adjustedFun = adjustFunction(f)
    import space._

    Iterator.iterate(state) { state => try {
        val (x,currentMomentum) = updateTheta(adjustedFun, state)
        //the Func used to update the theta, sub class overide it.
        val (value,grad) = calculateObjective(adjustedFun, x, state.history)
        val (adjValue,adjGrad) = adjust(x,grad,value)
        val oneOffImprovement = (state.adjustedValue - adjValue)/
          (state.adjustedValue.abs max adjValue.abs max 1E-6 * state.initialAdjVal.abs)
        logger.info(f"Val and Grad Norm: $adjValue%.6g (rel: " +
          f"$oneOffImprovement%.3g) ${norm(adjGrad)}%.6g")
        val history = updateHistory(x,grad,value, adjustedFun, state)
        val newCInfo = convergenceCheck.
          .update(x,
            grad,
            value,
            state,
            state.convergenceInfo)
        failedOnce = false
        FirstOrderMinimizerX
          .State(x,
            value,
            grad,
            adjValue,
            adjGrad,
            state.iter + 1,
            state.initialAdjVal,
            history,
            newCInfo,
            currentMomentum)
    } catch {
        case x: FirstOrderException if !failedOnce =>
          failedOnce = true
          logger.error("Failure! Resetting history: " + x)
          state.copy(history = initialHistory(adjustedFun, state.x))
        case x: FirstOrderException =>
          logger.error("Failure again! Giving up and returning. " +
            "Maybe the objective is just poorly behaved?")
          state.copy(searchFailed = true)
      }
    }
  }

  def iterations(f: DF, init: T): Iterator[State] = {
    val adjustedFun = adjustFunction(f)
    infiniteIterations(f, initialState(adjustedFun, init))
      .takeUpToWhere{s =>
      convergenceCheck.apply(s, s.convergenceInfo) match {
        case Some(converged) =>
          logger.info(s"Converged because ${converged.reason}")
          true
        case None =>
          false
      }
    }
  }

  def minimize(f: DF, init: T): T = {
    minimizeAndReturnState(f, init).x
  }


  def minimizeAndReturnState(f: DF, init: T): State = {
    iterations(f, init).last
  }
}


object FirstOrderMinimizerX {

  /**
   * Tracks the information about the optimizer, including the current point,
   * its value, gradient, and then any history.
   * Also includes information for checking convergence.
   * @param x the current point being considered
   * @param value f(x)
   * @param grad f.gradientAt(x)
   * @param adjustedValue  f(x) + r(x)
   * @param adjustedGradient f'(x) + r'(x)
   * @param iter what iteration number we are on.
   * @param initialAdjVal f(x_0) + r(x_0), used for checking convergence
   * @param history any information needed by the optimizer to do updates.
   * @param searchFailed did the line search fail?
   */
  case class State[+T, +ConvergenceInfo, +History](x: T,
                                                   value: Double,
                                                   grad: T,
                                                   adjustedValue: Double,
                                                   adjustedGradient: T,
                                                   iter: Int,
                                                   initialAdjVal: Double,
                                                   history: History,
                                                   convergenceInfo: ConvergenceInfo,
                                                   momentum: T,
                                                   searchFailed: Boolean = false) {
  }

  trait ConvergenceCheck[T] {
    type Info
    def initialInfo: Info
    def apply(state: State[T, _, _], info: Info): Option[ConvergenceReason]
    def update(newX: T,
               newGrad: T,
               newVal: Double,
               oldState: State[T, _, _],
               oldInfo: Info): Info
    def ||(otherCheck: ConvergenceCheck[T]):
    ConvergenceCheck[T] = orElse(otherCheck)

    def orElse(other: ConvergenceCheck[T]):
    ConvergenceCheck[T] = {
      SequenceConvergenceCheck(asChecks ++ other.asChecks)
    }

    protected def asChecks:
    IndexedSeq[ConvergenceCheck[T]] = IndexedSeq(this)
  }

  object ConvergenceCheck {
    implicit def fromPartialFunction[T](pf: PartialFunction[State[T, _, _], ConvergenceReason]):
    ConvergenceCheck[T] = new ConvergenceCheck[T] {
      override type Info = Unit

      def update(newX: T,
                 newGrad: T,
                 newVal: Double,
                 oldState: State[T, _, _],
                 oldInfo: Info):
      Info = oldInfo

      override def apply(state: State[T, _, _], info: Info):
      Option[ConvergenceReason] = pf.lift(state)

      override def initialInfo: Info = ()
    }
  }

  case class SequenceConvergenceCheck[T](checks: IndexedSeq[ConvergenceCheck[T]])
    extends ConvergenceCheck[T] {
    type Info = IndexedSeq[ConvergenceCheck[T]#Info]

    override def initialInfo: IndexedSeq[ConvergenceCheck[T]#Info] = checks.map(_.initialInfo)

    override def update(newX: T,
                        newGrad: T,
                        newVal: Double,
                        oldState: State[T, _, _],
                        oldInfo: Info):
    Info = {
      require(oldInfo.length == checks.length)
      (checks zip oldInfo).map { case (c, i) =>
        c.update(newX,
          newGrad,
          newVal,
          oldState,
          i.asInstanceOf[c.Info]) }
    }

    override def apply(state: State[T, _, _],
                       info: IndexedSeq[ConvergenceCheck[T]#Info]): Option[ConvergenceReason] = {
      (checks zip info).iterator.flatMap { case (c, i) =>
        c(state, i.asInstanceOf[c.Info])}.toStream.headOption
    }
  }


  trait ConvergenceReason {
    def reason: String
  }
  case object MaxIterations extends ConvergenceReason {
    override def reason: String = "max iterations reached"
  }

  case object GradientConverged extends ConvergenceReason {
    override def reason: String = "gradient converged"
  }

  def maxIterationsReached[T](maxIter: Int):
  ConvergenceCheck[T] = ConvergenceCheck.fromPartialFunction {
    case s: State[_, _, _] if (s.iter >= maxIter && maxIter >= 0) =>
      MaxIterations
  }

  def gradientConverged[T](tolerance: Double, relative: Boolean = true)
                          (implicit space: NormedModule[T, Double]): ConvergenceCheck[T] = {
    import space.normImpl
    ConvergenceCheck.fromPartialFunction[T] {
      case s: State[T, _, _] if (norm(s.adjustedGradient) < tolerance
        * (if (relative) s.adjustedValue else 1.0)) =>
        GradientConverged
    }
  }


  def defaultConvergenceCheckX[T](maxIter: Int, tolerance: Double,
                                 relative: Boolean = true, fvalMemory: Int = 20)
                                (implicit space: NormedModule[T, Double]): ConvergenceCheck[T] =
    (
      maxIterationsReached[T](maxIter) ||
      gradientConverged[T](tolerance, relative)
    )

}
