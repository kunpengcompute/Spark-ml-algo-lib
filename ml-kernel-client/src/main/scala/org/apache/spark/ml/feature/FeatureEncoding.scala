// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.feature

import org.apache.spark.sql.DataFrame

class FeatureEncoding extends Serializable{
  def execute(dataset: DataFrame = null): Unit = {}

  def setMapLoadPath(mapLoadPath: String): this.type = null

  def setDataPath(dataPath: String): this.type = null

  def setOutputFilePath(outputFilePath: String): this.type = null

  def setLocalSavePath(localSavePath: String): this.type = null

  def setEncodeColumns(encodeColumns: String): this.type = null

  def setNumThread(numThread: Int): this.type = null
}
