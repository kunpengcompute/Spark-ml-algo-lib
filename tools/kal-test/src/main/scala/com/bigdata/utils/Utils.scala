package com.bigdata.utils

import java.io.{File, FileInputStream, InputStreamReader, PrintWriter}
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Utils {
  /**
   *
   * @param filename The resource name
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


  /**
    *  Convert DenseMatrix to 2-dimension array, stored in row major
    * @param matrix Input matrix
    * @return 2-dimension array, stored in row major
    */
  def toRowMajorArray(matrix: DenseMatrix): Array[Array[Double]] = {
    val nRow = matrix.numRows
    val nCol = matrix.numCols
    val arr = new Array[Array[Double]](nRow).map(_ => new Array[Double](nCol))
    if(matrix.isTransposed){
      var srcOffset = 0
      for{i <- 0 until nRow} {
        System.arraycopy(matrix.values, srcOffset, arr(i), 0, nCol)
        srcOffset += nCol
      }
    } else {
      matrix.values.indices.foreach(idx => {
        val j = math.floor(idx / nRow).toInt
        val i = idx % nRow
        arr(i)(j) = matrix.values(idx)
      })
    }
    arr
  }

  def writeMatrix(mat: DenseMatrix, path: String): Unit = {
    val writer =new PrintWriter(path)
    val arr = toRowMajorArray(mat)
    arr.foreach(vec => writer.write(vec.mkString(",") + "\n"))
    writer.close()
  }

  def writeVector(vector: DenseVector, path: String): Unit = {
    val writer =new PrintWriter(path)
    vector.values.foreach(d => writer.write(d + "\n"))
    writer.close()
  }

  }
