// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.tuning

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap}

/**
 * Class of hyper-parameters space.
 */
class ParamSpace() {
  var paramList: List[ParamType[_ <: AnyVal]] = List()
  var paramNames: Array[Param[_ <: AnyVal]] = Array()

  /**
   * Add IntType hyper-parameters.
   *
   * @param parent parent object.
   * @param name param name.
   * @param valueRange Range of parameter values.
   */
  def addIntParam(parent: String, name: String, valueRange: BaseRange): Unit = {
    val param = IntParamType(valueRange, parent, name)
    paramList :+= param
    paramNames :+= param.getParamName
  }

  /**
   * Add DoubleType hyper-parameters.
   *
   * @param parent parent object.
   * @param name param name.
   * @param valueRange Range of parameter values.
   */
  def addDoubleParam(parent: String, name: String, valueRange: BaseRange): Unit = {
    val param = DoubleParmType(valueRange, parent, name)
    paramList :+= param
    paramNames :+= param.getParamName
  }

  private def asDouble(num: Any): Double = {
    num match {
      case i: Int => i.toDouble
      case i: Long => i.toDouble
      case i: Float => i.toDouble
      case i: Double => i
      case _ => throw new Exception(s"type ${num.getClass} is not supported")
    }
  }

  /**
   * Get configuration values from paramMaps.
   *
   * @param configs param configurations.
   * @return param value.
   */
  def getConfigsValue(configs: Array[ParamMap]): Array[Vector] = {
    val values: ArrayBuffer[Vector] = new ArrayBuffer[Vector]
    for {config <- configs} {
      var vectorArray: Array[Double] = Array()
      paramNames.foreach { paramNames =>
        vectorArray :+= asDouble(config(paramNames))
      }
      values.append(Vectors.dense(vectorArray))
    }
    values.toArray
  }

  /**
   * Get some recommended configurations.
   *
   * @param size configuration number.
   * @return configurations and configuration value vectors.
   */
  def getConfigurations(size: Int): (Array[ParamMap], Array[Vector]) = {
    val configs: ArrayBuffer[ParamMap] = new ArrayBuffer[ParamMap]
    val values: ArrayBuffer[Vector] = new ArrayBuffer[Vector]
    for {iter <- 1 to size} {
      val paramMap = ParamMap.empty
      var vectorArray: Array[Double] = Array()
      paramList.foreach(param => {
        val x = param.giveParamPair()
        paramMap.put(x)
        vectorArray :+= asDouble(x.value)

      })
      configs.append(paramMap)
      values.append(Vectors.dense(vectorArray))
    }
    (configs.toArray, values.toArray)
  }
}
