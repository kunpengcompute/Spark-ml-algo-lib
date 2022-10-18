package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.PrefixSpanVerify

import io.airlift.compress.lz4.Lz4Codec
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.util
import java.io.{File, FileWriter}
import java.util.HashMap
import scala.beans.BeanProperty


class PrefixSpanConfig extends Serializable {

  @BeanProperty var ps: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class PrefixSpanParams extends Serializable {

  @BeanProperty var numPartitions: Int = _
  @BeanProperty var minSupport: Double = _
  @BeanProperty var maxPatternLength: Int = _
  @BeanProperty var maxLocalProjDBSize: Int = _
  @BeanProperty var localTimeout: String = _
  @BeanProperty var filterCandidates: String = _
  @BeanProperty var projDBstep: String = _
  @BeanProperty var redistributeData: Boolean = _

  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var numFreqSeqs: Long = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object PrefixSpanRunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName , isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/ps/ps.yml")
      val representer = new Representer
      representer.addClassTag(classOf[PrefixSpanParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[PrefixSpanConfig]), representer, options)
      val description = new TypeDescription(classOf[PrefixSpanParams])
      yaml.addTypeDescription(description)
      val config: PrefixSpanConfig = yaml.load(stream).asInstanceOf[PrefixSpanConfig]
      val params = new PrefixSpanParams()
      val paramsMap = config.ps.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      params.setNumPartitions(paramsMap.get("numPartitions").asInstanceOf[Int])
      params.setMinSupport(paramsMap.get("minSupport").asInstanceOf[Double])
      params.setMaxPatternLength(paramsMap.get("maxPatternLength").asInstanceOf[Int])
      params.setMaxLocalProjDBSize(paramsMap.get("maxLocalProjDBSize").asInstanceOf[Int])
      params.setLocalTimeout(paramsMap.get("localTimeout").asInstanceOf[String])
      params.setFilterCandidates(paramsMap.get("filterCandidates").asInstanceOf[String])
      params.setProjDBstep(paramsMap.get("projDBstep").asInstanceOf[String])
      params.setRedistributeData(paramsMap.get("redistributeData").asInstanceOf[Boolean])
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("PrefixSpan")
      params.setSaveDataPath(s"${saveResultPath}/${params.algorithmName}/${datasetName}")
      params.setVerifiedDataPath(s"${params.saveDataPath}_raw")
      var appName = s"${params.algorithmName}_${datasetName}"
      if (isRaw.equals("yes")){
        appName = s"${params.algorithmName}_${datasetName}_raw"
        params.setVerifiedDataPath(params.saveDataPath)
        params.setSaveDataPath(s"${params.saveDataPath}_raw")
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf().setAppName(appName)
      if (isRaw.equals("no")) {
        conf.set("spark.boostkit.ml.ps.localTimeout", params.localTimeout)
        conf.set("spark.boostkit.ml.ps.filterCandidates", params.filterCandidates)
        conf.set("spark.boostkit.ml.ps.projDBstep", params.projDBstep)
      }
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val costTime = new PrefixSpanKernel().runJob(spark, params)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(PrefixSpanVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
        val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
        writerIsCorrect.write(s"${params.testcaseType} ${params.isCorrect} \n")
        writerIsCorrect.close()
      }

      val writer = new FileWriter(s"report/${params.testcaseType}_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${params.costTime}s; isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }

class PrefixSpanKernel {

  def runJob(spark: SparkSession, params: PrefixSpanParams): Double = {

    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val startTime = System.currentTimeMillis()
    val oriSequences = sc.textFile(params.dataPath).map{line =>
      var trans = Array.empty[Array[Int]]
      var items = Array.empty[Int]
      line.split(" ").foreach{itemStr =>
        val item = itemStr.toInt
        if (item >= 0) {
          items :+= item
        } else if (-1 == item) {
          trans :+= items
          items = Array.empty
        } else if (-2 == item) {
          assert(items.isEmpty)
        }
      }
      trans
    }

    val sequences = if (params.redistributeData) {
      sc.parallelize(oriSequences.collect(), params.numPartitions).cache()
    } else {
      oriSequences.repartition(params.numPartitions).cache()
    }
    println(s"Data count: ${sequences.count()}")
    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)
    val prefixSpan = new PrefixSpan()
        .setMinSupport(params.minSupport)
        .setMaxPatternLength(params.maxPatternLength)
        .setMaxLocalProjDBSize(params.maxLocalProjDBSize)
    val model = prefixSpan.run(sequences)

    val newPatterns = model.freqSequences.map{freqSequence =>
      val freqSeqs = freqSequence.sequence.map{v =>
         v.sorted.mkString("(", ",", ")")
      }.mkString("[", ",", "]")
      s"$freqSeqs,${freqSequence.freq}"
    }.persist()

    val numFreqSeqs = newPatterns.count()
    params.setNumFreqSeqs(numFreqSeqs)
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    PrefixSpanVerify.saveRes(newPatterns, params.saveDataPath, sc)
    costTime
  }
 }

}
