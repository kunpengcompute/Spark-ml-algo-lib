// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.tuning

import org.apache.spark.ml.param.{Param, ParamPair}

/**
 * Abstract base class for IntParamType and DoubleParmType.
 */
abstract class ParamType[T <: AnyVal] {
  /**
   * Sample one param from valueRange.
   * @return a param value.
   */
  def sampleOne(): T

  /**
   * get param name.
   * @return param name.
   */
  def getParamName: Param[T]

  /**
   * get param name and sample one param.
   * @return a param and its value.
   */
  def giveParamPair(): ParamPair[T]
}

/**
 * Param for int type values.
 *
 * @param valueRange range of param values.
 * @param parent parent object.
 * @param name param name.
 */
final case class IntParamType(valueRange: BaseRange, parent: String, name: String)
  extends ParamType[Int] {
  val paramName: Param[Int] = new Param(parent, name, "")

  /**
   * Sample one param from valueRange.
   * @return a param value.
   */
  override def sampleOne(): Int = valueRange.sampleOne().toInt

  /**
   * get param name.
   * @return param name.
   */
  override def getParamName: Param[Int] = paramName

  /**
   * get param name and sample one param.
   * @return a param and its value.
   */
  override def giveParamPair(): ParamPair[Int] = ParamPair(getParamName, sampleOne())
}

/**
 * Param for Double type values.
 *
 * @param valueRange range of param values.
 * @param parent parent object.
 * @param name param name.
 */
final case class DoubleParmType(valueRange: BaseRange, parent: String, name: String)
  extends ParamType[Double] {
  val paramName: Param[Double] = new Param(parent, name, "")

  /**
   * Sample one param from valueRange.
   * @return a param value.
   */
  override def sampleOne(): Double = valueRange.sampleOne()

  /**
   * get param name and sample one param.
   * @return a param and its value.
   */
  override def getParamName: Param[Double] = paramName

  /**
   * get param name and sample one param.
   * @return a param and its value.
   */
  override def giveParamPair(): ParamPair[Double] = ParamPair(paramName, sampleOne())
}
