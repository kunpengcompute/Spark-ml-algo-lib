// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.recommendation

import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}

trait SimRankParams extends Params {}

case class SimRankSimilarity(userSimilarity: DataFrame, itemSimilarity: DataFrame)

class SimRank(override val uid: String) extends SimRankParams {
  def this() = this(Identifiable.randomUID("SimRank"))

  def setDamp(value: Double): this.type = null

  def setNumIter(value: Int): this.type = null

  def setUserCol(value: String): this.type = null

  def setItemCol(value: String): this.type = null

  def computeSimilarity(dataset: Dataset[_]): SimRankSimilarity = null

  override def copy(extra: ParamMap): Params = null
}
