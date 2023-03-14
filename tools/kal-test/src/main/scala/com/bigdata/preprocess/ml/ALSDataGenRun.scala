package com.bigdata.preprocess.ml

import org.apache.spark.rdd.{RDD, PairRDDFunctions}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.random._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors,Vector}

object ALSDataGenRun extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ALSDataGenRun")
    val sc = new SparkContext(conf)

    var outputPath = ""
    var numUsers: Int = 100
    var numProducts: Int = 100
    var sparsity: Double = 0.05
    var implicitPrefs: Boolean = false
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val numPartitions = 200

    if (args.length == 5) {
      outputPath = args(0)
      numUsers = args(1).toInt
      numProducts = args(2).toInt
      sparsity = args(3).toDouble
      implicitPrefs = args(4).toBoolean

      println(s"Output Path: $outputPath")
      println(s"Num of Users: $numUsers")
      println(s"Num of Products: $numProducts")
      println(s"sparsity: $sparsity")
      println(s"Implicit Prefs: $implicitPrefs")
    } else {
      System.err.println(
        s"Usage: $ALSDataGenRun <OUTPUT_PATH> <NUM_USERS> <NUM_PRODUCTS> <NUM_RATINGS> <IMPLICITPREFS>"
      )
      System.exit(1)
    }

    val rawData: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, numUsers, numProducts, numPartitions)
    val rng = new java.util.Random()
    val data = rawData.map{v =>
      val a = Array.fill[Double](v.size)(0.0)
      v.foreachActive{(i, vi) =>
        if(rng.nextDouble <= sparsity) {
          a(i) = vi
        }
      }
      Vectors.dense(a).toSparse
    }
    data.saveAsObjectFile(outputPath)

    sc.stop()
  }
}