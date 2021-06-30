// scalastyle:off header.matches
/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.clustering

import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}

private[clustering] trait DBSCANParams extends Params with HasFeaturesCol with HasPredictionCol
  with HasLabelCol {
  def getMinPoints: Int = 0

  def getEpsilon: Double = 0d

  def getSampleRate: Double = 0d
}

class DBSCAN(override val uid: String) extends DBSCANParams {

  def this() = this(Identifiable.randomUID("DBSCAN"))

  def setMinPoints(value: Int): this.type = null

  def setEpsilon(value: Double): this.type = null

  def setSampleRate(value: Double): this.type = null

  def fitPredict(dataset: Dataset[_]): DataFrame = null

  override def copy(extra: ParamMap): DBSCAN = defaultCopy(extra)
}
