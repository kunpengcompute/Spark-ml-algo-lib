// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package com.intel.ssg.bdt.nlp

import java.io._

import org.apache.spark.rdd.RDD

trait VerboseMode

case object VerboseLevel1 extends VerboseMode

case object VerboseLevel2 extends VerboseMode

case class CRFModel (
    head: Array[String],
    dic: Array[(String, Int)],
    alpha: Array[Double]) extends Serializable {

  protected def formatVersion = "1.0"

  private var verboseMode: Option[VerboseMode] = None

  private var nBest = 0
  private var costFactor = 1.0

  def setNBest(nBest: Int): CRFModel = {
    this
  }

  def setVerboseMode(mode: VerboseMode): CRFModel = {
    this
  }

  def setcostFact(cf: Double): CRFModel = {
    this
  }

  override def toString: String = {
    null
  }

  def toStringHead: String = {
    null
  }

  def toArrayString: Array[String] = {
    null
  }

  def predict(tests: RDD[Sequence]): RDD[Sequence] = {
    null
  }

  def predict(tests: Array[Sequence]): Array[Sequence] = {
    null
  }

  def testCRF(test: Sequence,
              costFactor: Double,
              vMode: Option[VerboseMode]): Sequence = {
    null
  }
}

object CRFModel {
  def load(source: String): CRFModel = {
    null
  }

  def loadBinaryFile(path: String): CRFModel = {
    null
  }

  def loadArray(source: Array[String]): CRFModel = {
    null
  }

  def save(model: CRFModel): String = {
    null
  }

  def saveBinaryFile(model: CRFModel, path: String): Unit = {
  }

  def saveArray(model: CRFModel): Array[String] = {
    null
  }
}
