// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.ml.feature

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

class SPCA(override val uid: String)
  extends Estimator[PCAModel] with PCAParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("spca"))

  def setInputCol(value: String): this.type = null

  def setOutputCol(value: String): this.type = null

  def setK(value: Int): this.type = null

  def setMode(value: String): this.type = null

  override def fit(dataset: Dataset[_]): PCAModel = null

  override def transformSchema(schema: StructType): StructType = null

  override def copy(extra: ParamMap): SPCA = null
}
