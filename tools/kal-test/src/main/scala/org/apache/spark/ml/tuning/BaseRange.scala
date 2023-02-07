// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.tuning

import scala.util.Random

/**
 * Abstract base class for IntervalRange, ContinueRange and DiscreteRange.
 */
abstract class BaseRange() {
  val rd = new Random()

  /**
   * Sample a value from a range of values.
   * @return a value.
   */
  def sampleOne(): Double
}

/** Create a new range with the `start`, `end` and `step` values of this range.
 *
 * @param start the start value
 * @param end the end value
 * @param step the step value
 */
final case class IntervalRange(start: Double, end: Double, step: Double) extends BaseRange {
  require(end != start, s"Upper boundary $end must not be equal to boundary $start")
  require(step != 0.0, s"Step must not be equal to 0")

  val paramValues: List[Double] = (BigDecimal(start) to end by step).map(_.toDouble).toList

  /**
   * Sample a value from a range of values.
   * @return a value.
   */
  override def sampleOne(): Double = {
    paramValues(rd.nextInt(paramValues.length))
  }
}

/** Create a new range with the `lower` and `upper` values of this range.
 *
 * @param lower the start value
 * @param upper the end value
 */
final case class ContinueRange(lower: Double, upper: Double) extends BaseRange {
  require(upper > lower, s"Upper boundary $upper must be greater than lower boundary $lower")

  /**
   * sample a value from a range of values.
   * @return a value.
   */
  override def sampleOne(): Double = {
    lower + (upper - lower) * rd.nextDouble()
  }
}

/** Create a new range with the discrete values set.
 *
 * @param paramValues set of discrete values.
 */
final case class DiscreteRange(paramValues: Seq[Double]) extends BaseRange {

  /**
   * sample a value from a range of values.
   * @return a value.
   */
  override def sampleOne(): Double = {
    paramValues(rd.nextInt(paramValues.length))
  }
}
