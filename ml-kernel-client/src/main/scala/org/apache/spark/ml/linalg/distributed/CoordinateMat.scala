// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.ml.linalg.distributed

import org.apache.spark.rdd.RDD

object CoordinateMat {
  /**
   * To compute matrix multiplication A^T^ * B * A when A is sparse or small,
   * and matrix B must be square
   * @param matA matrix A
   * @param matB matrix B
   * @param numCores total number of cores
   * @param m number of rows of A and number of rows and columns of B
   * @param n number of columns of A
   * @return coo format distributed matrix
   */
  def computeAtBA(
      matA: RDD[(Int, Int, Double)],
      matB: RDD[(Int, Int, Double)],
      numCores: Int,
      m: Int,
      n: Int): RDD[(Int, Int, Double)] = null
}
