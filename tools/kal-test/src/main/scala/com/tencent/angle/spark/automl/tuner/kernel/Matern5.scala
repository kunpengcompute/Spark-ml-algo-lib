/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.spark.automl.tuner.kernel

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics._
import com.tencent.angel.spark.automl.tuner.math.SquareDist

/**
  * Matern covariance function with v = 5/2
  * (1 + sqrt(5)*r/l + 5r^2/(3l^2)) * exp(-sqrt(5)*r/l)
  * Here r is the distance |x1-x2| of two points
  * Hyper-parameter: l is the length scale
  */
case class Matern5() extends Covariance {

  /**
    * the covariance function
    *
    * @param x1
    * @param x2
    * @param params
    * @return
    */
  override def cov(x1: BDM[Double],
                   x2: BDM[Double],
                   params: BDV[Double]): BDM[Double] = {

    require(params.size == 1,
      s"Number of hyper parameters is ${params.length} while expected 1")

    val l = params(0)

    val distMat = SquareDist(x1, x2)
    val r = sqrt(distMat)

    val vPart = sqrt(5) * r / l + 5.0 / 3.0 * distMat / pow(l, 2) + 1.0
    val expPart = exp(-sqrt(5) * r / l)
    val covMatrix = vPart *:* expPart

    covMatrix
  }

  /**
    * the derivative of covariance function against kernel hyper-parameters
    *
    * @param x1
    * @param x2
    * @param params
    * @return
    */
  override def grad(x1: BDM[Double],
                    x2: BDM[Double],
                    params: BDV[Double]): Array[BDM[Double]] = {

    require(params.size == 1,
      s"Number of hyper parameters is ${params.length} while expected 1")

    val l = params(0)

    val distMat = SquareDist(x1, x2)
    val r = sqrt(distMat)

    val vPart = sqrt(5) * r / l + 5.0 / 3.0 * distMat / pow(l, 2) + 1.0
    val expPart = exp(-sqrt(5) * r / l)

    val vPartGrad = -(sqrt(5) * r / pow(l, 2) + 10.0 * distMat / (3.0 * pow(l, 3))) *:* expPart
    val expPartGrad = vPart *:* expPart *:* (sqrt(5) * r / pow(l, 2))

    val gradL = vPartGrad + expPartGrad

    Array(gradL)
  }
}
