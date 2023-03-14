package com.bigdata.compare.graph

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SubgraphMatchingVerify extends Serializable {

  val ARGS_NUM = 7
  def main(args: Array[String]): Unit = {

    var master = "local"
    var inputMatches = ""
    var inputType = ""
    var inputGraph = ""
    var splitGraph = ""
    var outputResult = ""
    var partitionNum = 1

    if (args.length < ARGS_NUM) {
      if (master != "local") {
        println("Wrong parameters, please check the input.")
      }
    } else {
      master = args(0)
      inputMatches = args(1)
      inputType = args(2)
      inputGraph = args(3)
      splitGraph = args(4)
      outputResult = args(5)
      partitionNum = args(6).toInt
    }

    FileSystem.get(new Configuration()).delete(new Path(outputResult), true)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getRootLogger.setLevel(Level.ERROR)

    val supportedTypes = Array[String]("4sqr", "4dgn", "5tree", "6star")
    require(supportedTypes.contains(inputType), "type not support, can only support 4sqr, 4dgn, 5tree and 6star")

    val sparkConf = new SparkConf().setMaster(master)
    if (master.contains("local")) {
      sparkConf.setAppName("Local test CheckResultSubgraphMatching")
    } else {
      sparkConf.setAppName("CheckResultSubgraphMatching test on" + inputMatches)
    }
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    val identicalMatches = loadExistingResults(sc, inputMatches, inputType)
    val edgesRDD = sc.textFile(inputGraph, partitionNum)
      .filter(line => line.nonEmpty && line.head != '#')
      .map { line =>
        val tokens: Array[String] = line.split(splitGraph)
        (tokens(0).toInt, tokens(1).toInt)
      }.flatMap(x => Iterator((x._1, x._2), (x._2, x._1)))
      .distinct()
      .filter(x => x._1 != x._2)
    val mGraphAdjMap = edgesRDD.groupBy(_._1)
      .mapValues(f => {f.map(x => x._2).toArray})
      .collectAsMap()

    val mGraphBC = sc.broadcast(mGraphAdjMap)

    val rst = identicalMatches.mapPartitions{ f => {
      val localGraph = mGraphBC.value
      var res = new ArrayBuffer[Boolean]()
      while (f.hasNext) {
        val matchedNodes = f.next()
        val tmp = checkCorrectness(inputType, matchedNodes, localGraph)
        res = res ++ Array(tmp)
      }
      res.toIterator
    }}

    rst.saveAsTextFile(outputResult)
    val falseNum = rst.filter(x => !x)
      .count()
    if (falseNum > 0) {
      println("there are %d instances are incorrect".format(falseNum))
    } else {
      println("all instances are correct")
    }

  }

  def instance2Nodes(edges: Array[String], inputTpye: String): Array[Int] = {

    val cooList = edges.flatMap{ edgeS =>
      var edge = edgeS.replaceAll("\\(", "")
      edge = edge.replaceAll("\\)", "")
      val node = edge.split(",").map(x => x.toLong)
      Iterator((node(0), node(1)), (node(1), node(0)))
    }
    val adjListMap = cooList.groupBy(_._1)
      .mapValues(f => {f.map(x => x._2)})
    val adjList = adjListMap.toArray
    val nodes = adjList.sortBy(-_._2.length).map(x => x._1.toInt)

    // 4sqr nodes: 0 -> 1 -> 2 -> 3 -> 0
    if (inputTpye == "4sqr") {
      if(adjListMap(nodes(0)).contains(nodes(3))) {
        val id = if (adjListMap(nodes(0)).contains(nodes(1))) 2 else 1
        val tmp = nodes(id)
        nodes(id) = nodes(3)
        nodes(3) = tmp
      }
    }

    // 5tree nodes: 0 -> 1 -> 4, 0 -> 2, 0 -> 3
    if (inputTpye == "5tree" ) {
      if(adjListMap(nodes(1)).contains(nodes(2)) || adjListMap(nodes(2)).contains(nodes(3))) {
        val tmp = nodes(4)
        if(adjListMap(nodes(1)).contains(nodes(2))) {
          nodes(4) = nodes(2)
          nodes(2) = tmp
        } else {
          nodes(4) = nodes(3)
          nodes(3) = tmp
        }
      }

    }

    nodes

  }

  def loadExistingResults(sc: SparkContext, path: String, inputType: String): RDD[Array[Int]] = {
    val existMatches = sc.textFile(path).flatMap(line => {
      val x = line.split("\t")
      val matchInstance = instance2Nodes(x, inputType)
      Iterator(matchInstance)
    })
    existMatches
  }

  def checkCorrectness(testType: String, nodes: Array[Int],
                       localGraph: scala.collection.Map[Int, Array[Int]]): Boolean = {

    var tag =false
    if (testType == "4sqr") {
      val check03 = !localGraph(nodes(0)).contains(nodes(3))
      val check12 = !localGraph(nodes(1)).contains(nodes(2))
      tag = check03 && check12
    }
    if (testType == "4dgn") {
      tag = !localGraph(nodes(2)).contains(nodes(3))
    }
    if (testType =="5tree") {
      val isDegreeCorrect = localGraph(nodes(0)).size == 3
      var leafNodeNeigh = new ArrayBuffer[Int]()
      var i = 2
      while (i < 5) {
        val tmpNeigh = localGraph(nodes(i))
        leafNodeNeigh = leafNodeNeigh ++ tmpNeigh
        i = i + 1
      }
      var isNeighCorrect = false
      i = 2
      while (i < 5) {
        isNeighCorrect = !leafNodeNeigh.contains(nodes(i))
        i = i + 1
      }
      isNeighCorrect = !localGraph(nodes(2)).contains(nodes(1)) &&
        !localGraph(nodes(3)).contains(nodes(1)) && !localGraph(nodes(4)).contains(nodes(0))
      tag = isDegreeCorrect && isNeighCorrect
    }
    if (testType == "6star") {
      val centerNode = nodes(0)
      val isDegreeCorrect = (localGraph(centerNode).size == 5)
      var leafNodeNeigh = new ArrayBuffer[Int]()
      var i = 1
      while (i < 6) {
        val tmpNeigh = localGraph(nodes(i))
        leafNodeNeigh = leafNodeNeigh ++ tmpNeigh
        i = i + 1
      }
      var isNeighCorrect = false
      i = 1
      while (i < 6) {
        isNeighCorrect = !leafNodeNeigh.contains(nodes(i))
        i = i + 1
      }
      tag = isDegreeCorrect && isNeighCorrect
    }
    tag
  }

}
