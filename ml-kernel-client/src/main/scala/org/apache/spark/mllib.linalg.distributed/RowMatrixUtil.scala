// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.mllib.linalg.distributed

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrix, SparseMatrix, SparseVector}
import org.apache.spark.rdd.RDD

object RowMatrixUtil {

  def selectSVDBranch(n: Int, k: Int): String = {
    null
  }

  def computeGramMatrixAsDenseMatrix(rows: RDD[SparseVector], nCols: Int): Matrix = {
    null
  }

  def computeGramMatrix(rows: RDD[SparseVector], nCols: Int)
  : (Int, Int, Array[((Int, Int), SparseMatrix)]) = {
    null
  }

  def multiplySparseGramMatrixBy
  (blockMatrix: (Int, Int, Array[((Int, Int), SparseMatrix)]))(v: BDV[Double]): BDV[Double] = {
    null
  }

  def multiplyDenseGramMatrixBy
  (blockMatrix: (Int, Int, Array[((Int, Int), BDM[Double])]))(v: BDV[Double]): BDV[Double] = {
    null
  }

  def toBlockMatrix(cov: BDM[Double], sc: SparkContext)
  : (Int, Int, Array[((Int, Int), BDM[Double])]) = {
    null
  }
}
