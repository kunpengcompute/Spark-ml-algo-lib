// scalastyle:off

package com.bigdata.graph
import com.bigdata.utils.Utils

import org.apache.spark.graphx.lib.{Fraudar, Parameters}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{BufferedWriter, File, FileWriter, InputStreamReader}
import java.util
import scala.beans.BeanProperty
import scala.collection.mutable
import scala.collection.mutable.Map

class FraudarConfig extends Serializable {
  @BeanProperty var fraudar: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class FraudarParams extends Serializable {
  @BeanProperty var splitGraph: String = _
  @BeanProperty var partitions: Int = _

  @BeanProperty var iSetOutPath: String = _
  @BeanProperty var jSetOutPath: String = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}


object FraudarRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName, isRaw) = (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val iSetOutPath = args(2)
      val jSetOutPath = args(3)
      val representer = new Representer
      representer.addClassTag(classOf[FraudarParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val stream: InputStreamReader = Utils.getStream("conf/graph/fraudar/fraudar.yml")
      val yaml = new Yaml(new Constructor(classOf[FraudarConfig]), representer, options)
      val description = new TypeDescription(classOf[FraudarParams])
      yaml.addTypeDescription(description)
      val config: FraudarConfig = yaml.load(stream).asInstanceOf[FraudarConfig]

      val params = new FraudarParams()
      val paramsMap =
        config.fraudar.get(isRaw match {
          case "no" => "opt"
          case _ => "raw"
        }).get(datasetName).asInstanceOf[util.HashMap[String, Object]]
      params.setSplitGraph(paramsMap.get("splitGraph").toString)
      params.setPartitions(paramsMap.get("partitions").toString.toInt)
      params.setDatasetName(datasetName)
      params.setDataPath(dataPath)
      params.setJSetOutPath(jSetOutPath)
      params.setISetOutPath(iSetOutPath)
      params.setIsRaw(isRaw)
      params.setAlgorithmName("Fraudar")
      params.setTestcaseType(s"${params.algorithmName}_${datasetName}_${isRaw}")

      val conf = new SparkConf().setAppName(params.testcaseType)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val costTime = isRaw match {
        case "no" => new FraudarKernel().runOptJob(spark, params)
        case "yes" => new FraudarKernel().runRawJob(spark, params)
      }

      params.setCostTime(costTime)
      println(s"Exec Successful: costTime: ${costTime}s")

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(
        s"report/${params.testcaseType}_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss", System.currentTimeMillis())}.yml")
      yaml.dump(params, writer)
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

class FraudarKernel {
  def runOptJob(spark: SparkSession, params: FraudarParams): Double = {
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val startTime = System.currentTimeMillis()
    val bipartGraph = Util.readUndirectDataFromHDFS(sc, params.dataPath, params.splitGraph, params.partitions)
      .map(f => (f._1.toLong, f._2.toLong))
      .persist(StorageLevel.MEMORY_ONLY_SER)
    bipartGraph.foreachPartition(f => {})

    val res = Fraudar.runFraudar(bipartGraph)
    res.map(f => f._1).distinct().saveAsTextFile(params.iSetOutPath)
    res.map(f => f._2).distinct().saveAsTextFile(params.jSetOutPath)

    val finishTime = System.currentTimeMillis()
    val costTime = (finishTime - startTime) / 1000
    costTime
  }

  def runRawJob(spark: SparkSession, params: FraudarParams): Double = {
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val startTime = System.currentTimeMillis()
    val bipartGraph = Util.readUndirectDataFromHDFS(sc, params.dataPath, params.splitGraph, params.partitions)
      .map(f => (f._1.toLong, f._2.toLong))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    bipartGraph.foreachPartition(f => {})

    val res = runFraudar(sc, bipartGraph)
    outputResult(params.iSetOutPath, params.jSetOutPath, res.toSet)

    val finishTime = System.currentTimeMillis()
    val costTime = (finishTime - startTime) / 1000
    costTime
  }

  /**
   * 开源
   * https://github.com/XinyaZhao/Social-Network-Fraud-Detection/blob/master/Analytics%20Code/Code_to_Analyze/frauder_v4.scala
   */
  /* get degree from a key-values pair */
  def getDegree(s: Tuple2[Long, Iterable[(Long, Long)]]): List[String] = {
    var myList = List[String]()
    for (e <- s._2) {
      val crt = "i" + e._2 + " j" + s._1 + " " + s._2.size //i dst点 j src点 " " scr的出度大小
      myList = crt :: myList
    }
    myList.reverse
  }

  /* cost function for column-weighting */
  def getCost(s: String): Double = {
    val degree = s.toDouble
    1 / (math.log(degree + 5))
  }

  def flat_degree(s: String): List[(String, Double)] = {
    var l = List[(String, Double)]()
    val i = s.split(" ")(0)
    val j = s.split(" ")(1)
    val cij = s.split(" ")(2)
    l = (i, getCost(cij)) :: l
    l = (j, getCost(cij)) :: l
    l.reverse
  }

  /* get a scala mutable map from a RDD which stores the "i, j, degree of ij" */
  def getCostMap(rdd: RDD[String]): RDD[(String, Double)] = {
    rdd.flatMap(flat_degree).reduceByKey((sum, n) => (sum + n))
  }

  /* Calcuate the f value of the whole set */
  def getSetValue(c: RDD[(String, Double)]): Double = {
    if (c.count != 0) {
      val v = c.reduce((a, b) => (" ", (a._2 + b._2)))._2
      v / 2
    }
    else {
      0.00
    }
  }

  /* get the vertex with minimum cost */
  def getDeleted(c: RDD[(String, Double)]): String = {
    if (c.count != 0) {
      val deleted = c.min()(new Ordering[Tuple2[String, Double]]() {
        override def compare(x: (String, Double), y: (String, Double)): Int =
          Ordering[Double].compare(x._2, y._2)
      })._1
      //println("deleted:------------------------- " + deleted)
      deleted
    }
    else {
      " "
    }
  }

  /* update each line with a deleted vertex */
  def update(sc:SparkContext, degree: RDD[String], d: String): RDD[String] = {
    var new_array = degree.collect;
    var tmp = new_array.to[mutable.ArrayBuffer]
    if (d.contains("j")) {
      new_array = new_array.filterNot(s => s.split(" ")(1) == d)
      tmp = new_array.to[mutable.ArrayBuffer]
    }
    if (d.contains("i")) {
      var update_j = List[String]()
      for (s <- new_array) {
        if (s.split(" ")(0) == d) {
          update_j = s.split(" ")(1) :: update_j
          tmp -= s
        }
      }

      val tmp_buffer = tmp.toArray
      // need a tmp buffert to deletee tmp
      for (j <- update_j) {
        for (s <- tmp_buffer) {
          if (s.split(" ")(1) == j) {
            tmp -= s
            val iszero = s.split(" ")(2).toInt - 1
            if (iszero != 0) {
              val new_line = s.split(" ")(0) + " " + s.split(" ")(1) + " " + (s.split(" ")(2).toInt - 1).toString
              tmp -= s
              tmp += new_line
            }
          }
        }
      }
    }
    sc.parallelize(tmp)
  }

  /* get graph from cost array*/
  def getGraph(degree: RDD[String]): List[String] = {
    var g = List[String]()
    for (c <- degree.collect) {
      if (!g.contains(c.split(" ")(0))) {
        g = c.split(" ")(0) :: g
      }
      if (!g.contains(c.split(" ")(1))) {
        g = c.split(" ")(1) :: g
      }
    }
    g
  }

  /* iterative delete a vertex */
  def greedyDecreasing(sc:SparkContext, degree: RDD[String]): List[String] = {
    val cost = getCostMap(degree)
    val value = getSetValue(cost) / cost.count
    var valueMap = Map[Int, Double]()
    valueMap += (0 -> value)
    var graph = List[List[String]]()
    graph = getGraph(degree) :: graph
    var new_degree = degree
    var c = cost
    var a = 0
    while (c.count != 0) { // not cost size, need to be the number of vertex
      val iter1 = System.currentTimeMillis()
      println("c.count : " + c.count)
      a = a + 1
      val d = getDeleted(c)
      new_degree = update(sc, new_degree, d)
      //newDegree = update(deleted) //update the degree of remaining i and j based on the deteled vertex
      c = getCostMap(new_degree)
      graph = getGraph(new_degree) :: graph
      val value = getSetValue(c) / c.count // the set vaule should be divided by the |C|
      //println("value : " + value)
      //new_degree.foreach(println)
      //println(getGraph(c))
      valueMap += (a -> value)

      val iter2 = System.currentTimeMillis()
      println(s"iterNum:${a}, updatetime: "+ ((iter2 - iter1) / 1000.0) + " sec")
    }
    var max_index = -1
    var max_Value = -1.000
    for (s <- valueMap) {
      if (s._2 > max_Value) {
        max_index = s._1
        max_Value = s._2
      }
    }
    //println("maxvalue" + " " + max_Value + " index:" + max_index)
    //graph.reverse.foreach(println)
    val objectGraph = graph.reverse(max_index)

    //objectGraph.foreach(f=>println(f))
    objectGraph
  }

  /* get the most density graph*/
  def getFinalSet(cst: Map[String, Double], dgr: List[String]): Set[String] = {
    var set = Set[String]()
    for (e <- cst) {
      set += (e._1)
    }
    set -- dgr.toSet
  }

  def outputResult(iset_out:String, jset_out:String, set: Set[String]): Unit = {
    val ibf = new BufferedWriter(new FileWriter(iset_out));
    val jbf = new BufferedWriter(new FileWriter(jset_out));
    val sorted_list = set.toList.sortWith(_.substring(1).toLong < _.substring(1).toLong)
    for (s <- sorted_list) {
      if (s.contains("i")) {
        ibf.write(s + "\n");
      }
      else {
        jbf.write(s + "\n");
      }
    }
    ibf.flush()
    jbf.flush()
    ibf.close()
    jbf.close()
  }

  def runFraudar(sc:SparkContext, bipartGraph: RDD[(Long, Long)]):List[String] = {
    val pairs = bipartGraph.map(x => (x._2, x._1))
    val group = pairs.groupBy(x => x._1)
    val degree = group.flatMap(getDegree)
    greedyDecreasing(sc, degree)
  }
}
