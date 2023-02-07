// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.feature

import java.io.File
import java.net.URI
import java.util.Date

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.spark.ml.StaticUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}

class FeatureEncodingOrigin extends Serializable{
  var mapLoadPath = ""
  var dataPath = ""
  var outputFilePath = ""
  var localSavePath = ""
  var encodeColumns = Array[String]()
  var numThread = 40

  def setMapLoadPath(mapLoadPath: String): this.type = {
    this.mapLoadPath = mapLoadPath
    this
  }

  def setDataPath(dataPath: String): this.type = {
    this.dataPath = dataPath
    this
  }

  def setOutputFilePath(outputFilePath: String): this.type = {
    this.outputFilePath = outputFilePath
    this
  }

  def setLocalSavePath(localSavePath: String): this.type = {
    this.localSavePath = localSavePath
    this
  }

  def setEncodeColumns(encodeColumns: String): this.type = {
    this.encodeColumns = encodeColumns.split(",")
    this
  }

  def setNumThread(numThread: Int): this.type = {
    this.numThread = numThread
    this
  }

  def parseJsonToIntMap(json: String): mutable.Map[String, Int] = {
    val mapper = new ObjectMapper()
    val node = mapper.readValue(json, classOf[java.util.HashMap[String, Int]])
    node.asScala
  }

  def loadJsonToString(path: String): String = {
    Source.fromFile(path, "utf-8").mkString
  }

  def padZero(input: Array[Int], maxLength: Int): Array[Int] = {
    if (input.length > maxLength) {
      input.dropRight(input.length-maxLength)
    } else {
      input.++(Array.ofDim[Int](maxLength-input.length))
    }
  }

  def transform(input: DataFrame, featureMapKey: String,
                featureMap: Map[String, Int], inputCol: String*): DataFrame = {
    if (featureMap.isEmpty) {
      throw new Exception("featureMap is empty")
    }

    val suffixName = "_index"
    val transformUDF = udf((maxLengthKey: String, value: String) => {
      val transformList = ArrayBuffer[Int]()
      if (featureMap.contains(featureMapKey + "," + value)) {
        transformList.append(featureMap(featureMapKey + "," + value))
      } else {
        // use 1 as feature index if not found
        transformList.append(1)
      }

      // return the maxLength array
      padZero(transformList.toArray, 1)
    })

    var data = input
    for (cols <- inputCol) {
      data = data.withColumn(
        cols + suffixName,
        transformUDF(
          lit(cols),
          col(cols)
        ))
    }
    data
  }

  def dirDel(path: File) {
    if (!path.exists()) {
      return
    }
    if (path.isFile()) {
      path.delete()
      return
    }
    val file: Array[File] = path.listFiles()
    for (d <- file) {
      dirDel(d)
    }
    path.delete()
  }

  def copyFileToLocal(spark: SparkSession, hdfsPath: String, localPath: String): Unit = {
    val localFilePath = new File(localPath)
    dirDel(localFilePath)
    if (!localFilePath.exists()) {
      localFilePath.mkdirs()
    }
    val fs = FileSystem.get(new URI(hdfsPath), new Configuration())
    val fileArray = FileUtil.stat2Paths(fs.listStatus(new Path(hdfsPath)))
    val startTime = new Date().getTime
    for (cnt <- 1 to fileArray.length) {
      fs.copyToLocalFile(fileArray(cnt-1), new Path(localPath))
    }
  }

  def execute(dataset: DataFrame = null): Unit = {
    require(mapLoadPath.nonEmpty, "mapLoadPath is empty")
    require(dataPath.nonEmpty, "dataPath is empty")
    require(outputFilePath.nonEmpty, "outputFilePath is empty")
    require(localSavePath.nonEmpty, "localSavePath is empty")
    require(numThread > 0, "numThread is illegal")
    val featureMap = parseJsonToIntMap(loadJsonToString(mapLoadPath))
    var res = dataset
    for(feature <- encodeColumns) {
      require(res.columns.contains(feature), "non existent encodeColumns: " + feature)
      res = transform(res, feature, featureMap.toMap, feature)
    }
    res
      .select(encodeColumns.map{t => col(t + "_index")}: _*)
      .write.mode("overwrite")
      .save(outputFilePath)

    copyFileToLocal(res.sparkSession, outputFilePath, localSavePath + "encode")
    copyFileToLocal(res.sparkSession, dataPath, localSavePath + "data")
  }
}
