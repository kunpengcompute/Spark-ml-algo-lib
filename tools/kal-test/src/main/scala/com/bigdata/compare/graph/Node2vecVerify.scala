package com.bigdata.compare.graph

import scala.util.Try

import com.bigdata.graph.Util
import smile.validation.AUC;

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.{SparkConf, SparkContext};

object Node2vecVerify {
  def main(args: Array[String]): Double = {
    val graphPath = args(0) // dataset path
    val negEdgePath = args(1) // groundTruth path
    val embeddingPath = args(2) // output path
    val partNum = args(3).toInt
    val weighted = false
    val sparkConf = new SparkConf().setAppName("Node2vecVerify")
    val sc = SparkContext.getOrCreate(sparkConf)

    val edgesRdd = Util.readEdgeListFromHDFS(sc, graphPath, "\t", weighted, partNum)
    val negativeEdgesRdd = Util.readUndirectDataFromHDFS(sc, negEdgePath, ",", partNum)
    val nvModel: collection.Map[Long, Vector] = Util.get(Util.readNode2VecModel(sc, embeddingPath))
    val nvModelBC = sc.broadcast(nvModel)
    edgesRdd.foreachPartition(_ => nvModelBC.value)
    val positiveEdgesScores: Array[Double] = edgesRdd.flatMap({
      case (src, dst, weight) =>
        Try(Iterator.single(Util.distCos(nvModelBC.value(src).toArray, nvModelBC.value(dst).toArray)))
          .getOrElse(Iterator.empty)
    }).filter(score => !score.isInfinity && !score.isNaN)
      .collect()
    val negativeEdgesScores: Array[Double] = negativeEdgesRdd
      .flatMap({
        case (src, dst) =>
          Try(Iterator.single(Util.distCos(nvModelBC.value(src.toLong).toArray, nvModelBC.value(dst.toLong).toArray)))
            .getOrElse(Iterator.empty)
      }).filter(score => !score.isInfinity && !score.isNaN)
      .collect()
    val truths = Array.fill(positiveEdgesScores.length)(1) ++ Array.fill(negativeEdgesScores.length)(0)
    val auc = AUC.of(truths, (positiveEdgesScores ++ negativeEdgesScores))
    val flag = auc >= 0.90
    sc.stop()
    println(s"Link Prediction AUC score = $auc")
    println(s"The algorithm is correct: ${flag}")
    return auc
  }
}
