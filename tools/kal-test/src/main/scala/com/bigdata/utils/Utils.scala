package com.bigdata.utils

import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Utils {
  /**
   *
   * @param file The resource name
   * @return
   */

  def getStream(filename: String): InputStreamReader = {

    val file = new File(filename)
    if (!file.exists() || file.isDirectory) {
      throw  new Exception(s"Fail to find prorerty file[${file}]")
    }
    val inputStreamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)

    inputStreamReader

    }

  /**
   *
   * @param dateFmt date format
   * @param utcMilliseconds "yyyy-MM-dd HH:mm:ss"
   * @return String date
   */
  def getDateStrFromUTC(dateFmt: String, utcMilliseconds: Long): String = {
    val sf = new SimpleDateFormat(dateFmt)
    sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    sf.format(new Date(utcMilliseconds))
  }

  }
