package com.bigdata.compare.graph

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MsspVerify {
  def loadGraph(sc: SparkContext,
                input: String,
                isWeighted: Boolean,
                split: String,
                partNum: Int): RDD[(Long, ArrayBuffer[(Int, Double)])] = {
    val inputRdd = sc.textFile(input, partNum).flatMap(line => {
      if (null == line || line.startsWith("#")) {
        Iterator.empty
      } else {
        var m_line = line
        if (m_line.contains("(") || m_line.contains(")")) {
          m_line = m_line.replaceAll("\\(", "")
          m_line = m_line.replaceAll("\\)", " ")
        }
        val x = m_line.split(split)
        if (x.length < 2) {
          Iterator.empty
        } else {
          val a = x(1).split((" "))
          val b = new ArrayBuffer[(Int, Double)]()
          a.map(f => {
            val c = f.split(",")
            b.append((c(0).toInt, c(1).toDouble))
          })
          Iterator((x(0).toLong, b))
        }
      }
    })
    inputRdd
  }

  def main(args: Array[String]): Unit = {
    val path0 = args(0)
    val path1 = args(1)
    val partNum = 232
    val split = ":"
    val sparkConf = new SparkConf().setAppName("MsspVerify")
    val sc = SparkContext.getOrCreate(sparkConf)
    val rdd0 = loadGraph(sc, path0, true, split, partNum).map(x => (x._1, x._2))
    val rdd1 = loadGraph(sc, path1, true, split, partNum).map(x => (x._1, x._2))
    val res = rdd0.join(rdd1).map(f => {
      f._2._1.sortBy(_._1).sameElements(f._2._2.sortBy(_._1))
    }).distinct().collect()

    val cnt0 = rdd0.count()
    val cnt1 = rdd1.count()
    val flag = !res.contains(false)
    sc.stop()
    println(s"Static Nodes Count: $cnt0, $cnt1")
    println(s"The algorithm is correct: ${flag}")
  }
}
