// scalastyle:off header.matches
/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */

package breeze.optimize

import breeze.math._
import breeze.numerics._
import breeze.util._

/**
 * Implements the Orthant-wise Limited Memory QuasiNewton method,
 * which is a variant of LBFGS that handles L1 regularization.
 *
 * Paper is Andrew and Gao (2007) Scalable Training of L1-Regularized Log-Linear Models
 *
 * @author dlwh
 */
class OWLQNX[K, T](maxIter: Int, m: Int, l1reg: K => Double, tolerance: Double)
                  (implicit space: MutableEnumeratedCoordinateField[T, K, Double])
  extends LBFGSX[T](maxIter, m, tolerance = tolerance) with SerializableLogging {

  def this(maxIter: Int, m: Int, l1reg: K => Double)
          (implicit space: MutableEnumeratedCoordinateField[T, K, Double])
            = this(maxIter, m, l1reg, 1E-8)

  def this(maxIter: Int, m: Int, l1reg: Double, tolerance: Double = 1E-8)
          (implicit space: MutableEnumeratedCoordinateField[T, K, Double])
            = this(maxIter, m, (_: K) => l1reg, tolerance)

  def this(maxIter: Int, m: Int, l1reg: Double)
          (implicit space: MutableEnumeratedCoordinateField[T, K, Double])
            = this(maxIter, m, (_: K) => l1reg, 1E-8)

  def this(maxIter: Int, m: Int)(implicit space: MutableEnumeratedCoordinateField[T, K, Double])
            = this(maxIter, m, (_: K) => 1.0, 1E-8)

  require(m > 0)

  import space._

  override def chooseDescentDirection(state: State, fn: DiffFunction[T]): T = {
    val descentDir = super.chooseDescentDirection(state.copy(grad = state.adjustedGradient), fn)
    val correctedDir = space.zipMapValues.map(descentDir, state.adjustedGradient, { case (d, g)
    => if (d * g < 0) d else 0.0 })

    correctedDir
  }


  // projects x to be on the same orthant as y
  // this basically requires that x'_i = x_i if sign(x_i) == sign(y_i), and 0 otherwise.

  override def takeStep(state: State, dir: T, stepSize: Double): T = {
    val stepped = state.x + dir * stepSize
    val orthant = computeOrthant(state.x, state.adjustedGradient)
    space.zipMapValues.map(stepped, orthant, { case (v, ov) =>
      v * I(math.signum(v) == math.signum(ov))
    })
  }

  // Adds in the regularization stuff to the gradient
  override def adjust(newX: T, newGrad: T, newVal: Double): (Double, T) = {
    var adjValue = newVal
    val res = space.zipMapKeyValues.mapActive(newX, newGrad, {case (i, xv, v) =>
      val l1regValue = l1reg(i)
      require(l1regValue >= 0.0)

      if(l1regValue == 0.0) {
        v
      } else {
        adjValue += Math.abs(l1regValue * xv)
        xv match {
          case 0.0 =>
            val delta_+ = v + l1regValue
            val delta_- = v - l1regValue
            if (delta_- > 0) delta_- else if (delta_+ < 0) delta_+ else 0.0
          case _ => v + math.signum(xv) * l1regValue
        }
      }
    })
    adjValue -> res
  }

  private def computeOrthant(x: T, grad: T) = {
    val orth = space.zipMapValues.map(x, grad, {case (v, gv) =>
      if (v != 0) math.signum(v)
      else math.signum(-gv)
    })
    orth
  }

}
