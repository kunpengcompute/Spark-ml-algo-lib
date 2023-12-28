// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package com.intel.ssg.bdt.nlp

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

trait Regularization

case object L1 extends Regularization

case object L2 extends Regularization

class CRF private (
    private var freq: Int,
    private var regParam: Double,
    private var maxIterations: Int,
    private var tolerance: Double,
    private var regularization: Regularization) extends Serializable with Logging {

  def this() = this(
    freq = 1,
    regParam = 0.5,
    maxIterations = 1000,
    tolerance = 1E-3,
    regularization = L2)

  def setRegParam(regParam: Double): this.type = null

  def setFreq(freq: Int): this.type = null

  def setMaxIterations(maxIterations: Int): this.type = null

  def setTolerance(tol: Double): this.type = null

  def setRegularization(reg: Regularization): this.type = null

  def setCalcAccuracy(ca: Boolean): this.type = null

  def setCompLevel(compLevel: Int): this.type = null

  def setGlobalStageIterFraction(globalStageIterFraction: Double): this.type = null

  def setCommFreeSplit(commFreeSplit: Int): this.type = null

  def setCommFreeToleranceFactor(commFreeToleranceFactor: Double): this.type = null

  def setNumThread(numThread: Int): this.type = null

  def runCRF(
      template: Array[String],
      trains: RDD[Sequence],
      testArrayWithLabel: Array[Sequence] = Array[Sequence](),
      testArrayWithoutLabel: Array[Sequence] = Array[Sequence]()): CRFModel = null
}


case class Sequence (sequence: Array[Token]) extends Serializable {
  var seqProb = 0.0
  lazy val candidates = ArrayBuffer.empty[Sequence]

  def setSeqProb(seqProb: Double): Sequence = {
    this
  }

  def setCandidates(nBest: ArrayBuffer[Array[Int]],
    probN: ArrayBuffer[Double],
    labels: ArrayBuffer[String]): Sequence = {
    this
  }

  def Print(): String = {
    null
  }

  def nthPrint(k: Int): String = {
    null
  }

  def nBestPrint(): String = {
    null
  }

  override def toString: String = {
    null
  }

  def toArray: Array[Token] = sequence

  def compare(other: Sequence): Int = {
    null.asInstanceOf[Int]
  }

  def probPrinter(): String = {
    null
  }

}

object Sequence {
  def deSerializer(s: String): Sequence = {
    null
  }
  def serializer(sequence: Sequence): String = {
    null
  }
}

class Token(
    val label: String,
    val tags: Array[String]) extends Serializable {
  var prob : Array[(String, Double)] = null

  def setProb(probMat: Array[(String, Double)]): Token = {
    this
  }

  def probPrinter(): String = {
    null
  }

  override def toString: String = {
    null
  }

  def compare(other: Token): Int = {
    null.asInstanceOf[Int]
  }
}

object Token {

  def deSerializer(s: String): Token = {
    null
  }

  def serializer(token: Token): String = {
    null
  }

  def put(label: String, tags: Array[String]): Token = {
    null
  }

  def put(tags: Array[String]): Token = {
    null
  }
}
