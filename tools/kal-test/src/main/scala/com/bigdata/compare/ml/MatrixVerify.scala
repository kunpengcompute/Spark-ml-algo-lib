package com.bigdata.compare.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.sql.SparkSession

import java.io.FileWriter

object MatrixVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val sparkConf =  new SparkConf().setAppName("MatrixVerify")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val isCorrect = compareRes(path0, path1, spark)
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"${isCorrect}:\n${path0}\n${path1}\n")
    writerIsCorrect.close()
    println(s"The algorithm is correct: ${isCorrect}")

  }

  def saveMatrix(mat: DenseMatrix, saveDataPath: String, sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val saveFile = new Path(saveDataPath)
    if (fs.exists(saveFile)) {
      fs.delete(saveFile, true)
    }
    val res = toRowMajorArray(mat)
    sc.parallelize(res.map(_.mkString(";"))).saveAsTextFile(saveDataPath)
  }
  

  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val res1File = new Path(path0)
    val res2File = new Path(path1)
    if (fs.exists(res1File) && fs.exists(res2File)) {
      val res1 = sc.textFile(path0).map(line => line.split(";").map(_.toDouble)).collect()
      val res2 = sc.textFile(path1).map(line => line.split(";").map(_.toDouble)).collect()
      if (isEqualMatrix(res1, res2)) {
        return "true"
      }
      else {
        return "false"
      }
    }
    else{
      return "invaildComparison"
    }
  }

  def isEqualMatrix(res1: Array[Array[Double]], res2: Array[Array[Double]]): Boolean = {
    if (res1.length != res2.length)
      return false
    for (i <- res2.indices) {
      if (res1(i).length != res2(i).length)
        return false
      for (j <- res1(i).indices) {
        if (math.abs(math.abs(res1(i)(j)) - math.abs(res2(i)(j))) > 1e-6)
          return false
      }
    }
    true
  }

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
}