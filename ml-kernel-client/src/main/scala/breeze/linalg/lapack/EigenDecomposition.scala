// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package breeze.linalg.lapack

import breeze.linalg.{DenseMatrix, DenseVector}

object EigenDecomposition {

  final case class Eigen(var eigenVectors: DenseMatrix[Double],
                         var eigenValues: DenseVector[Double]) {
  }

  def symmetricEigenDecomposition(mat: DenseMatrix[Double]): Eigen = {
    null
  }
}
