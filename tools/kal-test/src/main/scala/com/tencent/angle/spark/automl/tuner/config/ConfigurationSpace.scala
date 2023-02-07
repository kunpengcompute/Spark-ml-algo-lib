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


package com.tencent.angel.spark.automl.tuner.config

import com.tencent.angel.spark.automl.tuner.TunerParam
import com.tencent.angel.spark.automl.tuner.math.BreezeOp._
import com.tencent.angel.spark.automl.tuner.parameter.{ContinuousSpace, ParamSpace}
import com.tencent.angel.spark.automl.utils.AutoMLException
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.reflect.ClassTag

class ConfigurationSpace(
                          val name: String,
                          private var paramDict: Map[String, ParamSpace[AnyVal]] = Map()) {

  val LOG: Log = LogFactory.getLog(classOf[ConfigurationSpace])

  var numParams: Int = paramDict.size

  var fields: ArrayBuffer[StructField] = new ArrayBuffer[StructField]()

  var param2Idx: Map[String, Int] = paramDict.keys.zipWithIndex.toMap
  var param2Doc: Map[String, String] = paramDict.map { case (k: String, v: ParamSpace[AnyVal]) => (k, v.doc) }
  var idx2Param: Map[Int, String] = param2Idx.map(_.swap)

  // param name -> param type (continuous or discrete), value type (int, double,...)
  val paramType: mutable.Map[String, (String, String)] = new mutable.HashMap[String, (String, String)]()

  // configurations tried
  var preX: HashSet[Vector] = HashSet[Vector]()
  var gridValues: Array[Configuration] = Array.empty
  var gridIndice = 0

  def getParamNum: Int = numParams

  def addParams(params: List[ParamSpace[AnyVal]]): Unit = {
    params.foreach(addParam)
  }

  def addParam[T <: AnyVal : ClassTag](param: ParamSpace[T]): Unit = {
    if (!paramDict.contains(param.name)) {
      fields += DataTypes.createStructField(param.name, DataTypes.DoubleType, false)
      paramType += param.name -> (param.pType, param.vType)
      paramDict += (param.name -> param)
      param2Idx += (param.name -> numParams)
      param2Doc += (param.name -> param.doc)
      idx2Param += (numParams -> param.name)
      numParams += 1
    }
    println(s"add param ${param.toString}, current params: ${paramDict.keySet.mkString(",")}")
  }

  def addParamType(pName: String, pType: String, vType: String): Unit = {
    if (!paramType.contains(pName))
      paramType += pName -> (pType, vType)
  }

  def getParamType(pName: String): (String, String) = {
    if (paramType.contains(pName))
      paramType(pName)
    else
      throw new AutoMLException(s"param $pName not exists in the configuration space.")
  }

  def getFields: Array[StructField] = fields.toArray

  def getParams(): Array[ParamSpace[AnyVal]] = paramDict.values.toArray

  def getParamByName(name: String): Option[ParamSpace[AnyVal]] = paramDict.get(name)

  def getIdxByParam(name: String): Option[Int] = param2Idx.get(name)

  def getParamByIdx(idx: Int): Option[ParamSpace[AnyVal]] = paramDict.get(idx2Param.getOrElse(idx, "none"))

  def getDocByName(name: String): Option[String] = param2Doc.get(name)

  def addHistories(vecs: Array[Vector]): Unit = preX ++= vecs

  def addHistory(vec: Vector): Unit = preX += vec

  def setAllToGrid(): Unit = {
    getParams().foreach {
      case cParam: ContinuousSpace =>
        if (!cParam.isGrid) cParam.resetGrid(TunerParam.defaultGridSize)
      case _ =>
    }
  }

  def spaceSize(): Int = {
    var size: Int = if (numParams > 0) 1 else 0
    var hasInfinite = false
    getParams().foreach { param =>
      param.numValues match {
        case Int.MaxValue => hasInfinite = true
        case _ => size *= param.numValues
      }
    }
    if (hasInfinite) Int.MaxValue else size
  }

  def sample(size: Int): Array[Configuration] = {
    var configs: ArrayBuffer[Configuration] = new ArrayBuffer[Configuration]

    var missing: Int = 0
    val left = if (spaceSize() == Int.MaxValue) Int.MaxValue else spaceSize - preX.size
    val trueSize = left min size
    println(s"configuration space size ${spaceSize()}, remaining $left, sample $trueSize")
    do {
      missing = trueSize - configs.length
      val vectors: Array[Vector] = Array.fill(missing)(Vectors.dense(new Array[Double](numParams)))
      param2Idx.foreach { case (paramName, paramIdx) =>
        paramDict.get(paramName) match {
          case Some(param) =>
            param.sample(missing).map(asDouble).zipWithIndex.foreach { case (f: Double, i: Int) =>
              vectors(i).toArray(paramIdx) = f
            }
          case None => LOG.info(s"Cannot find $paramName.")
        }
      }
      val validVectors = vectors.filter(isValid)
      validVectors.foreach { vec =>
        configs += new Configuration(param2Idx, param2Doc, vec)
      }
    } while (configs.length < trueSize)

    configs.toArray
  }

  def randomSample(size: Int): Array[Configuration] = {
    var configs: ArrayBuffer[Configuration] = new ArrayBuffer[Configuration]

    var missing: Int = 0
    val left = if (spaceSize() == Int.MaxValue) Int.MaxValue else spaceSize - preX.size
    val trueSize = left min size
    println(s"configuration space size ${spaceSize()}, remaining $left, sample $trueSize")
    do {
      missing = trueSize - configs.length
      val vectors: Array[Vector] = Array.fill(missing)(Vectors.dense(new Array[Double](numParams)))
      param2Idx.foreach { case (paramName, paramIdx) =>
        paramDict.get(paramName) match {
          case Some(param) =>
            param.sample(missing).map(asDouble).zipWithIndex.foreach { case (f: Double, i: Int) =>
              vectors(i).toArray(paramIdx) = f
            }
          case None => LOG.info(s"Cannot find $paramName.")
        }
      }
      val validVectors = vectors.filter(isValid)
      validVectors.foreach { vec =>
        configs += new Configuration(param2Idx, param2Doc, vec)
      }
    } while (configs.length < trueSize)
    configs.toArray
  }

  def gridSample(size: Int): Array[Configuration] = {
    if (gridValues.isEmpty) {
      gridValues = getGridConfigs()
    }
    val startIndice = gridIndice
    val endIndice = (gridIndice + size) min gridValues.size
    println(s"configuration space size ${gridValues.size}, " +
      s"remaining ${gridValues.size - startIndice}, sample from $startIndice to $endIndice")
    gridIndice = endIndice
    if (startIndice == gridValues.size) {
      Array.empty
    } else {
      val ret = new Array[Configuration](endIndice - startIndice)
      Array.copy(gridValues, startIndice, ret, 0, endIndice - startIndice)
      ret
    }
  }

  def getGridConfigs(): Array[Configuration] = {
    //assert(spaceSize() < Int.MaxValue, "all parameters must be discrete!")
    //println(s"configuration space size ${spaceSize()}")
    var configs: ArrayBuffer[Configuration] = new ArrayBuffer[Configuration]

    var tmp: ArrayBuffer[Array[Double]] = new ArrayBuffer[Array[Double]]

    val params = getParams()

    params.foreach {
      tmp += _.getValues
    }

    val paramsArray: Array[Array[Double]] = tmp.toArray

    if (numParams == 1) {
      var tmp: ArrayBuffer[Vector] = new ArrayBuffer[Vector]
      paramsArray.head.foreach {
        tmp += Vectors.dense(_)
      }
      val paramsVec = tmp.toArray
      paramsVec.filter(isValid).foreach { vec =>
        configs += new Configuration(param2Idx, param2Doc, vec)
      }
      configs.toArray
    } else if (numParams == 2) {
      val paramsGrid: Array[Array[Double]] = cartesian(paramsArray(0), paramsArray(1))
      var tmp: ArrayBuffer[Vector] = new ArrayBuffer[Vector]
      paramsGrid.foreach {
        tmp += Vectors.dense(_)
      }
      val paramsVec: Array[Vector] = tmp.toArray
      paramsVec.filter(isValid).foreach { vec =>
        configs += new Configuration(param2Idx, param2Doc, vec)
      }
      configs.toArray
    } else {
      var paramsGrid: Array[Array[Double]] = cartesian(paramsArray(0), paramsArray(1))

      paramsArray.foreach { a =>
        if (!(a sameElements paramsArray(0)) && !(a sameElements paramsArray(1))) {
          paramsGrid = cartesian(paramsGrid, a)
        }
      }

      var tmp: ArrayBuffer[Vector] = new ArrayBuffer[Vector]
      paramsGrid.foreach {
        tmp += Vectors.dense(_)
      }
      val paramsVec: Array[Vector] = tmp.toArray
      paramsVec.filter(isValid).foreach { vec =>
        configs += new Configuration(param2Idx, param2Doc, vec)
      }
      configs.toArray
    }
  }

  def asDouble(num: AnyVal): Double = {
    num match {
      case i: Int => i.toDouble
      case i: Long => i.toLong
      case i: Float => i.toDouble
      case i: Double => i
      case _ => throw new AutoMLException(s"type ${num.getClass} is not supported")
    }
  }

  def isValid(vec: Vector): Boolean = !preX.contains(vec)
}