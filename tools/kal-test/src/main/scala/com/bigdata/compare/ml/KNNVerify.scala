package com.bigdata.compare.ml

import java.io.{FileWriter, PrintWriter}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.neighbors.KNN
import org.apache.spark.sql.{Row, SparkSession}

object KNNVerify extends Serializable {

  /**
   * Calculate euclidean distance.
   */
  def euclideanDistance(v1: Vector, v2: Vector): Double = {
    euclideanDistance(v1.toArray, v2.toArray)
  }

  def euclideanDistance(v1: Array[Double], v2: Array[Double]): Double = {
    math.sqrt(v1.indices.map(i => math.pow(v1(i) - v2(i), 2)).sum)
  }

  /**
   * 通过暴力求解，得到KNN的真实解
   */
  def writeResult(args: Array[String]): Unit = {
    var exeTime = System.currentTimeMillis()

    val spark = SparkSession
      .builder()
      .appName("writeResults")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // 算法参数
    var pt, k, testNum, testBatchSize = -1
    var dataPath = ""

    // 待写入的真实解本地路径
    var groundTruthLocalPath = ""

    args.sliding(2, 2).foreach {
      case Array("--pt", value) => pt = value.toInt
      case Array("--k", value) => k = value.toInt
      case Array("--testNum", value) => testNum = value.toInt
      case Array("--testBatchSize", value) => testBatchSize = value.toInt
      case Array("--dataPath", value) => dataPath = value.toString
      case Array("--groundTruthLocalPath", value) => groundTruthLocalPath = value.toString
      case _ =>
    }

    // read data
    val rawData = sc.textFile(dataPath)
      .map(line => {
        val arr = line.split("\t")
        val id = arr(0).toLong
        val feature = Vectors.dense(arr(1).split(",").map(_.toDouble))
        (id, feature)
      }).cache()

    // split train/test datasets
    val trainData = rawData.filter(_._1 >= testNum).repartition(pt).cache()
    val testData = rawData.filter(_._1 < testNum).repartition(pt).cache()
    println(s"-------- split data, trainNum=${trainData.count()}, testNum=${testData.count()} ----------")
    rawData.unpersist(blocking = true)

    // search in batch
    for(startIdx <- 0 until testNum by testBatchSize) {
      exeTime = System.currentTimeMillis()
      val endIdx = math.min(startIdx + testBatchSize, testNum)
      val queryLocal = testData.filter(x => x._1 >= startIdx && x._1 < endIdx).collect()
      val queryBd = sc.broadcast(queryLocal)
      val neighbors = trainData.mapPartitions(iter => {
        val curTrainData = iter.toArray
        Iterator(queryBd.value.map{case (queryIdx, queryVector) => {
          val distances = curTrainData.map{case (trainIdx, trainVector) =>
            (trainIdx, euclideanDistance(trainVector, queryVector))}
            .sortBy(t => (t._2, t._1)).take(k)
          (queryIdx, distances)
        }})
      }).treeReduce((arr1, arr2) => {
        arr1.indices.toArray.map(i => {
          (arr1(i)._1, (arr1(i)._2 ++ arr2(i)._2).sortBy(t => (t._2, t._1)).take(k))
        })
      }, depth = 3)
      val writer = new PrintWriter(s"${groundTruthLocalPath}/part-${startIdx}-${endIdx}")
      neighbors.foreach{case(queryIdx, queryNN) => {
        writer.write(queryIdx + "\t" + queryNN.map(_._1).mkString(",") + "\n")
      }}
      writer.close()
      println(s"------ $startIdx-$endIdx done, time=${(System.currentTimeMillis() - exeTime) / 60000.0} ---------")
      queryBd.destroy()
    }

    spark.stop()
  }

  def verify(args: Array[String]): Unit = {
    val exeTime = System.currentTimeMillis()
    val spark = SparkSession
      .builder()
      .appName("writeResults")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // 算法参数
    var pt, k, testNum, testBatchSize = -1
    var dataPath = ""
    var datasetName = ""

    // 真实解hdfs路径
    var groundTruthHDFSPath = ""

    args.sliding(2, 2).foreach {
      case Array("--pt", value) => pt = value.toInt
      case Array("--k", value) => k = value.toInt
      case Array("--testNum", value) => testNum = value.toInt
      case Array("--testBatchSize", value) => testBatchSize = value.toInt
      case Array("--dataPath", value) => dataPath = value.toString
      case Array("--dataset_name", value) => datasetName = value.toString
      case Array("--groundTruthHDFSPath", value) => groundTruthHDFSPath = value.toString
      case _ =>
    }

    // read data
    import spark.implicits._
    val rawData = spark.sparkContext.textFile(dataPath)
      .map(line => {
        val arr = line.split("\t")
        val id = arr(0).toLong
        val feature = Vectors.dense(arr(1).split(",").map(_.toDouble))
        (id, feature)
      }).toDF("id", "features").cache()

    // split train/test datasets
    val trainDataDF = rawData.filter($"id" >= testNum).repartition(pt).cache()
    val testDataDF = rawData.filter($"id" < testNum).repartition(pt).cache()
    trainDataDF.count()
    testDataDF.count()
    rawData.unpersist(blocking = true)

    // fit
    val model = new KNN()
      .setFeaturesCol("features")
      .setAuxiliaryCols(Array("id"))
      .fit(trainDataDF)

    // transform
    val testResults = model
      .setNeighborsCol("neighbors")
      .setDistanceCol("distances")
      .setK(k)
      .setTestBatchSize(testBatchSize)
      .transform(testDataDF).cache()
    testResults.count()
    println(s"trainTime=${(System.currentTimeMillis() - exeTime) / 1000.0}")

    // 与groudtruth对比
    val trueResult = sc.textFile(groundTruthHDFSPath).map(line => {
      val arr = line.split("\t")
      val id = arr(0).toLong
      val neighbors = arr(1).split(",").map(_.toInt)
        .filter(neighborIdx => neighborIdx >= testNum).take(k)
      (id, neighbors)
    }).filter(_._2.length == k)
    val combinedData = trueResult.toDF("id", "trueNeighbors")
      .join(testResults.selectExpr("id", "neighbors", "distances"), "id")
      .map(r => {
        val trueNN = r.getAs[Seq[Int]]("trueNeighbors").toArray
        val myNN = r.getAs[Seq[Row]]("neighbors").map(_.getAs[Long]("id").toInt).toArray
        val myDistancesSize = r.getAs[Seq[Double]]("distances").toSet.size
        (r.getAs[Long]("id"), trueNN, myNN, myDistancesSize)
      })
      .filter(_._4 == k)
      .cache()
    //    val actualTotalNum = combinedData.count()

    val incorrectCases = combinedData.map{case (id, trueNN, myNN, _) => {
      val myNNSet = myNN.toSet
      var isEqual = true
      Range(0, k - 1).foreach(i => {
        if(!myNNSet.contains(trueNN(i)))
          isEqual = false
      })
      (id, isEqual, trueNN, myNN)
    }}.filter(!_._2).collect()

    var isCorrect = ""
    if(incorrectCases.length == 0)
      isCorrect = "correct"
    else
      isCorrect = "incorrect"
    println(s"Exec Successful: isCorrect: ${isCorrect}")
    val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
    writerIsCorrect.write(s"KNN_${datasetName} ${isCorrect} \n")
    writerIsCorrect.close()
    spark.stop()
  }

  def main(args: Array[String]): Unit = {

    var task: String = ""
    args.sliding(2, 2).foreach {
      case Array("--task", value) => task = value.toString.toLowerCase()
      case _ =>
    }
    require(Array("write", "verify").contains(task), s"Task name should be one of [write, verify], but got $task")

    task match {
      case "write" => writeResult(args)
      case "verify" => verify(args)
    }
  }
}
