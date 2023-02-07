package com.bigdata.compare.ml

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.sql.SparkSession

import java.io.FileWriter

object SVDVerify {
  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val sparkConf =  new SparkConf().setAppName("SVDVerify")
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

  def saveVector(vec: DenseVector, saveDataPath: String, sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val saveFile = new Path(saveDataPath)
    if (fs.exists(saveFile)) {
      fs.delete(saveFile, true)
    }
    val res = vec.toArray
    sc.parallelize(res).repartition(1).saveAsTextFile(saveDataPath)
  }


  def compareRes(path0: String, path1: String, spark: SparkSession): String = {
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val sigma1File = new Path(s"${path0}/s")
    val sigma2File = new Path(s"${path1}/s")
    val V1File = new Path(s"${path0}/V")
    val V2File = new Path(s"${path1}/V")
    if (fs.exists(sigma1File) && fs.exists(sigma2File) && fs.exists(V1File) && fs.exists(V2File)) {
      val s1 = sc.textFile(s"${path0}/s").map(_.toDouble).collect()
      val s2 = sc.textFile(s"${path1}/s").map(_.toDouble).collect()
      val V1 = sc.textFile(s"${path0}/V").map(line => line.split(";").map(_.toDouble)).collect()
      val V2 = sc.textFile(s"${path1}/V").map(line => line.split(";").map(_.toDouble)).collect()
      if (isEqualVector(s1, s2) && isEqualMatrix(V1, V2)) {
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

  def isEqualVector(optres: Array[Double], rawres: Array[Double]): Boolean = {
    val res1 = optres.sorted
    val res2 = rawres.sorted
    if (res1.length != res2.length)
      return false
    for (i <- res2.indices) {
      if ((math.abs(res1(i) - res2(i)) / res1(i)) > 0.001)
        return false
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