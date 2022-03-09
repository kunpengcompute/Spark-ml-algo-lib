package com.bigdata.ml

import java.io.{File, FileWriter}
import java.util.HashMap

import com.bigdata.utils.Utils
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

import scala.beans.BeanProperty


class PrefixSpanConfig extends Serializable {

  @BeanProperty var ps: HashMap[String, Object] = _
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
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var datasetCpuName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var evaluation: String = "mismatch"
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var numFreqSeqs: Long = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

class PrefixSpanKernel {

  def runJob(spark: SparkSession, params: PrefixSpanParams): Unit = {

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
    params.setCostTime(costTime)
    if (params.isRaw == "yes") {
      val verifiedFile = new Path(params.verifiedDataPath)
      if (fs.exists(verifiedFile)){
        val histPatterns = sc.sequenceFile[NullWritable, String](params.verifiedDataPath).map(_._2).persist()
        if (histPatterns.subtract(newPatterns).take(1).isEmpty &&
          newPatterns.subtract(histPatterns).take(1).isEmpty) {
          params.setEvaluation("match")
        }
      } else {
        params.setEvaluation("no verified data")
      }
    } else {
      val saveFile = new Path(params.saveDataPath)
      if (fs.exists(saveFile)) {
        fs.delete(saveFile, true)
      }
      newPatterns.repartition(15).map((NullWritable.get(), _))
        .saveAsSequenceFile(params.saveDataPath, Some(classOf[Lz4Codec]))
    }
  }
}

object PrefixSpanRunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName) =
        (modelConfSplit(0), modelConfSplit(1))

      val dataPath = args(1)

      val datasetCpuName = s"${datasetName}_${platformName}"
      val stream = Utils.getStream("conf/ml/ps/ps.yml")
      val representer = new Representer
      representer.addClassTag(classOf[PrefixSpanParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[PrefixSpanConfig]), representer, options)
      val description = new TypeDescription(classOf[PrefixSpanParams])
      yaml.addTypeDescription(description)
      val config: PrefixSpanConfig = yaml.load(stream).asInstanceOf[PrefixSpanConfig]
      val paramsMap = config.ps.get(datasetCpuName).asInstanceOf[HashMap[String, Object]]

      val params = new PrefixSpanParams()

      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setDatasetCpuName(datasetCpuName)
      params.setAlgorithmName("PrefixSpan")

      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)
      params.setMinSupport(paramsMap.get("minSupport").toString.toDouble)
      params.setMaxPatternLength(paramsMap.get("maxPatternLength").toString.toInt)
      params.setMaxLocalProjDBSize(paramsMap.get("maxLocalProjDBSize").toString.toInt)
      params.setLocalTimeout(paramsMap.get("localTimeout").toString)
      params.setFilterCandidates(paramsMap.get("filterCandidates").toString)
      params.setProjDBstep(paramsMap.get("projDBstep").toString)
      params.setRedistributeData(paramsMap.get("redistributeData").toString.toBoolean)
      params.setSaveDataPath(paramsMap.get("saveDataPath").toString)
      params.setVerifiedDataPath(paramsMap.get("verifiedDataPath").toString)

      var appName = s"PrefixSpan_${datasetName}_${platformName}"
      params.setTestcaseType(appName)

      val conf = new SparkConf()
        .setAppName(s"PrefixSpan_${datasetName}_${platformName}")

      if (platformName == "raw") {
        params.setIsRaw("yes")
      } else {
        conf.set("spark.boostkit.ml.ps.localTimeout", params.localTimeout)
        conf.set("spark.boostkit.ml.ps.filterCandidates", params.filterCandidates)
        conf.set("spark.boostkit.ml.ps.projDBstep", params.projDBstep)
      }

      val spark = SparkSession.builder.config(conf).getOrCreate()

      val ps = new PrefixSpanKernel()
      ps.runJob(spark, params)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/PrefixSpan${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${params.costTime}s; evaluation: ${params.evaluation}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }

}
