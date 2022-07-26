package com.bigdata.compare.ml

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.sql.{DataFrame, SparkSession}

object Word2VecEvaluation extends Serializable {

  def meanVectors(vectors: Array[Array[Float]], startIdx: Int, endIdx: Int): Array[Float] = {
    val vectorSize = vectors(0).length
    val result = new Array[Float](vectorSize)
    Range(startIdx, endIdx).foreach(i => blas.saxpy(vectorSize, 1.0f, vectors(i), 1, result, 1))
    blas.sscal(vectorSize, (1.0 / (endIdx - startIdx)).toFloat, result, 1)
    result
  }

  def meanVectors(vectors: Array[Array[Float]]): Array[Float] = {
    meanVectors(vectors, 0, vectors.length)
  }

  def cosDistance(x: Array[Double], y: Array[Double]): Double = {
    var sim, normX, normY = 0.0
    for (i <- x.indices) {
      sim += x(i) * y(i)
      normX += math.pow(x(i), 2)
      normY += math.pow(y(i), 2)
    }
    sim / math.sqrt(normX * normY)
  }

  def evaluateTaobaoProductCTR(
                                spark: SparkSession,
                                downstreamTrainFile: String,
                                downstreamTestFile: String,
                                w2vModel: Word2VecModel,
                                pt: Int): Double = {
    val sc = spark.sparkContext
    val bcW2vModel = sc.broadcast(w2vModel.getVectors)

    // 构造特征: [u, v, |u - v|]
    import spark.implicits._
    val trainData = sc.objectFile[(Double, Array[String])](downstreamTrainFile, pt)
      .map { case (label, sentence) =>
        val localW2vModel = bcW2vModel.value
        val featureVectors = sentence.map(localW2vModel)
        val feature = meanVectors(featureVectors, 0, featureVectors.length - 1).map(_.toDouble)
        val target = featureVectors.last.map(_.toDouble)
        val distance = cosDistance(feature, target)
        (label, Vectors.dense(feature ++ target ++ Array(distance)))
      }.repartition(pt)
      .toDF("label", "features")
      .cache()
    val testData = sc.objectFile[(Double, Array[String])](downstreamTestFile, pt)
      .map { case (label, sentence) =>
        val featureVectors = sentence.map(bcW2vModel.value)
        val feature = meanVectors(featureVectors, 0, featureVectors.length - 1).map(_.toDouble)
        val target = featureVectors.last.map(_.toDouble)
        val distance = cosDistance(feature, target)
        (label, Vectors.dense(feature ++ target ++ Array(distance)))
      }.repartition(pt)
      .toDF("label", "features")
      .cache()

    // 训练二分类模型
    val model = new LogisticRegression().fit(trainData)
    trainData.unpersist()

    // 评测：ROC
    val predictionAndLables = model.transform(testData)
      .select("prediction", "label").rdd
      .map(row => (row.getAs[Double](0), row.getAs[Double](1)))
    new BinaryClassificationMetrics(predictionAndLables).areaUnderROC()
  }

  def evaluateAlibabaCTR(
                          spark: SparkSession,
                          fieldNames: Array[String],
                          downstreamTrainFile: String,
                          downstreamTestFile: String,
                          w2vModels: Array[Word2VecModel],
                          pt: Int): Double = {
    val sc = spark.sparkContext
    val bcW2vModels = w2vModels.map(m => sc.broadcast(m.getVectors))
    val fieldNameIdMap = Map("item" -> 0, "cate" -> 1, "shop" -> 2, "node" -> 3, "product" -> 4, "brand" -> 5)
    val bcFieldIds = sc.broadcast(fieldNames.map(fieldNameIdMap))

    // 构造特征: [u, v, |u - v|]
    val trainData = readAlibabaDownstreamData(spark, bcFieldIds, downstreamTrainFile, bcW2vModels, pt).cache()
    val testData = readAlibabaDownstreamData(spark, bcFieldIds, downstreamTestFile, bcW2vModels, pt).cache()

    // 训练二分类模型
    val model = new LogisticRegression().fit(trainData)
    trainData.unpersist()

    // 评测：ROC
    val predictionAndLabels = model.transform(testData)
      .select("prediction", "label").rdd
      .map(row => (row.getAs[Double](0), row.getAs[Double](1)))
    new BinaryClassificationMetrics(predictionAndLabels).areaUnderROC()
  }

  def readAlibabaDownstreamData(spark: SparkSession, bcFieldIds: Broadcast[Array[Int]], downstreamFile: String,
                                bcW2vModels: Array[Broadcast[Map[String, Array[Float]]]], pt: Int): DataFrame = {
    val sc = spark.sparkContext
    val vectorSize = bcW2vModels(0).value.valuesIterator.next().length

    import spark.implicits._
    sc.objectFile[(Double, Array[(String, Array[String])])](downstreamFile, pt)
      .map { case (label, fieldVectors) =>
        (label, bcFieldIds.value.map(id => fieldVectors(id)))
      }.map { case (label, fieldVectors) =>
      val fieldNum = fieldVectors.length
      val features = new Array[Double](fieldNum * (2 * vectorSize + 1))
      var offset = 0
      for (i <- 0 until fieldNum) {
        val adVector = bcW2vModels(i).value(fieldVectors(i)._1).map(_.toDouble)
        val historyVector = meanVectors(fieldVectors(i)._2.map(bcW2vModels(i).value)).map(_.toDouble)
        System.arraycopy(adVector, 0, features, offset, vectorSize)
        System.arraycopy(historyVector, 0, features, offset + vectorSize, vectorSize)
        features(offset + 2 * vectorSize) = cosDistance(adVector, historyVector)
        offset += 2 * vectorSize + 1
      }
      (label, Vectors.dense(features))
    }.repartition(pt)
      .toDF("label", "features")
  }
}
