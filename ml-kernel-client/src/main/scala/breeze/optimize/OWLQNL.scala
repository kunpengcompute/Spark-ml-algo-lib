// scalastyle:off header.matches
/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package breeze.optimize

import breeze.linalg.DenseVector
import breeze.util.SerializableLogging

class OWLQNL(
    maxIterations: Int,
    maxFCalls: Int,
    m: Int,
    fTolerance: Double,
    xTolerance: Double,
    var l1RegParam: DenseVector[Double])
  extends FirstOrderMinimizer[DenseVector[Double], DiffFunction[DenseVector[Double]]](null)
    with SerializableLogging {

  def this(
      maxIterations: Int,
      m: Int,
      fTolerance: Double,
      l1RegParam: DenseVector[Double]) = {
    this(maxIterations, 0, m, fTolerance, 0.0, l1RegParam)
  }

  override def iterations(f: DiffFunction[DenseVector[Double]], x0: DenseVector[Double]):
  Iterator[FirstOrderMinimizer.State[DenseVector[Double], convergenceCheck.Info, History]] = {
    null
  }

  case class History()

  override def initialHistory(
      f: DiffFunction[DenseVector[Double]],
      init: DenseVector[Double]): History = {
    null
  }

  override def updateHistory(
      newX: DenseVector[Double],
      newGrad: DenseVector[Double],
      newVal: Double,
      f: DiffFunction[DenseVector[Double]],
      oldState: State): History = {
    null
  }

  override def chooseDescentDirection(state: State, f: DiffFunction[DenseVector[Double]]):
  DenseVector[Double] = {
    null
  }

  override def determineStepSize(
      state: State,
      f: DiffFunction[DenseVector[Double]],
      direction: DenseVector[Double]): Double = {
    0.0
  }

  override def takeStep(
      state: State,
      dir: DenseVector[Double],
      stepSize: Double): DenseVector[Double] = {
    null
  }
}
