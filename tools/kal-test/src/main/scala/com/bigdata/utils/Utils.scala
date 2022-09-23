package com.bigdata.utils

import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{File, FileInputStream, InputStreamReader, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._

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

  def checkDirs(dirName: String): Unit ={
    val folder = new File(dirName)
    if (!folder.exists()) {
      val mkdir = folder.mkdirs()
      println(s"Create dir report ${mkdir}")
    }
  }

  def saveEvaluation(res: Double, savePath: String, sc: SparkContext): Unit ={
    val result = new Array[String](1)
    result(0) = res.toString
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val saveFile = new Path(savePath)
    if (fs.exists(saveFile)) {
      fs.delete(saveFile, true)
    }
    sc.parallelize(result).repartition(1).saveAsTextFile(savePath)
  }

  def compareDoubleResults(saveDataPath: String, verifiedDataPath: String): String = {
    if(Files.exists(Paths.get(verifiedDataPath))){
      val saveFile = Source.fromFile(saveDataPath)
      val verifiedFile = Source.fromFile(verifiedDataPath)
      val pri = saveFile.getLines().toArray
      val opt = verifiedFile.getLines().toArray
      saveFile.close()
      verifiedFile.close()
      if (math.abs(pri(0).toDouble - opt(0).toDouble) / pri(0).toDouble <= 0.005) {
        return "correct"
      }
      else {
        return "incorrect"
      }
    }else{
      return "invaildComparison"
    }
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

  def readMatrix(path: String): Array[Array[Double]] = {
    val file = Source.fromFile(path)
    val arr = file.getLines().map(line => line.split(",").map(_.toDouble)).toArray
    file.close()
    arr
  }

  def isEqualMatrix(opensourceMatrix: Array[Array[Double]], boostkitMatrix: Array[Array[Double]], tol: Double = 1e-6): Boolean = {
    if(opensourceMatrix.length != boostkitMatrix.length)
      return false
    for(i <- boostkitMatrix.indices) {
      if(opensourceMatrix(i).length != boostkitMatrix(i).length)
        return false
      for(j <- opensourceMatrix(i).indices) {
        if(math.abs(math.abs(opensourceMatrix(i)(j)) - math.abs(boostkitMatrix(i)(j))) > tol)
          return false
      }
    }
    true
  }

}
