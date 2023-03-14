package com.bigdata.ml

import com.bigdata.utils.Utils
import com.bigdata.compare.ml.UpEvaluationVerify

import com.intel.ssg.bdt.nlp.{CRF, L2, Sequence, Token, Regularization}
import org.apache.spark.nlp.{CRF => CRFRaw, L2 => L2Raw, Sequence => Seq, Token => Tok}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{File, FileWriter}
import java.util
import scala.beans.BeanProperty

class CRFConfig extends Serializable {
  @BeanProperty var crf: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class CRFParams extends Serializable {
  @BeanProperty var pt: Int = _
  @BeanProperty var maxIter: Int = _
  @BeanProperty var regParam: Double = _
  @BeanProperty var freq: Int = _
  @BeanProperty var compLevel: Int = _
  @BeanProperty var nThread: Int = _
  @BeanProperty var tol: Double = _
  @BeanProperty var calAcc: Boolean = _
  @BeanProperty var templatePath: String = _


  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var saveDataPath: String = _
  @BeanProperty var verifiedDataPath: String = _
  @BeanProperty var ifCheck: String = _
  @BeanProperty var isCorrect: String = _
}

object CRFRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, isRaw, ifCheck) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2))
      val dataPath = args(1)
      val cpuName = args(2)
      val saveResultPath = args(3)

      val stream = Utils.getStream("conf/ml/crf/crf.yml")
      val representer = new Representer
      representer.addClassTag(classOf[CRFParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[CRFConfig]), representer, options)
      val description = new TypeDescription(classOf[CRFParams])
      yaml.addTypeDescription(description)
      val configs: CRFConfig = yaml.load(stream).asInstanceOf[CRFConfig]
      val params = new CRFParams()
      val paramsMap: util.HashMap[String, Object] = configs.crf.get(isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }).get(datasetName)
      params.setPt(paramsMap.getOrDefault("pt", "276").asInstanceOf[Int])
      params.setMaxIter(paramsMap.getOrDefault("maxIter", "500").asInstanceOf[Int])
      params.setRegParam(paramsMap.getOrDefault("regParam", "0.01").asInstanceOf[Double])
      params.setFreq(paramsMap.getOrDefault("freq", "0.20").asInstanceOf[Int])
      params.setCompLevel(paramsMap.getOrDefault("compLevel", "0").asInstanceOf[Int])
      params.setNThread(paramsMap.getOrDefault("nThread", "1").asInstanceOf[Int])
      params.setTol(paramsMap.getOrDefault("tol", "0.001").asInstanceOf[Double])
      params.setCalAcc(paramsMap.getOrDefault("calAcc", "false").asInstanceOf[Boolean])
      params.setTemplatePath(paramsMap.getOrDefault("templatePath", "/").asInstanceOf[String])

      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setIfCheck(ifCheck)
      params.setAlgorithmName("CRF")
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

      val spark = SparkSession.builder.config(conf).getOrCreate()
      val costTime = new CRFKernel().runJob(spark, params)
      params.setCostTime(costTime)

      Utils.checkDirs("report")
      if(ifCheck.equals("yes")){
        params.setIsCorrect(UpEvaluationVerify.compareRes(params.saveDataPath, params.verifiedDataPath, spark))
        val writerIsCorrect = new FileWriter(s"report/ml_isCorrect.txt", true)
        writerIsCorrect.write(s"${params.testcaseType} ${params.isCorrect} \n")
        writerIsCorrect.close()
      }

      val writer = new FileWriter(s"report/${params.testcaseType}_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s;isCorrect: ${params.isCorrect}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}

class CRFKernel {
  def runJob(spark: SparkSession, params: CRFParams): Double = {
    import spark.implicits._
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val dataPath = params.dataPath
    val pt = params.pt
    val maxIteration = params.maxIter
    val regParam = params.regParam
    val frequency = params.freq
    val compLevel = params.compLevel
    val nThread = params.nThread
    val tol = params.tol
    val calAcc = params.calAcc
    val templatePath = params.templatePath

    val t1 = System.currentTimeMillis()
    println("\n--------start--------\n")
    println("* start:             " + t1)

    // Load and parse the data
    val templates: Array[String] = sc.textFile(templatePath).toLocalIterator.filter(_.nonEmpty).toArray
    val testData = sc.textFile(dataPath + ".t").toLocalIterator.filter(_.nonEmpty).toArray

    val t2 = System.currentTimeMillis()
    println("* loading & pre-preprocess time:  ", (t2 - t1) / 1000.0 )

    var predictTime = 0.0
    if (params.isRaw == "no") {
      println("================== Running Opt CRF ==================")

      val trainDataOpt = sc.textFile(dataPath + ".tr").filter(_.nonEmpty).map { sentence =>
        val tokens = sentence.split("\t")
        Sequence(tokens.map { token =>
          val tags: Array[String] = token.split('|')
          Token.put(tags.last, tags.dropRight(1))
        })
      }

      var testArrayWithoutLabelOpt: Array[Sequence] = Array[Sequence]()
      var testArrayWithLabelOpt: Array[Sequence] = Array[Sequence]()

      val trainRDDOpt = sc.parallelize(trainDataOpt.collect(), pt).repartition(pt).cache()
      //println("number of training sequences: " + trainRDDOpt.count())

      testArrayWithoutLabelOpt = testData.map(sentence => {
        val tokens = sentence.split("\t")
        Sequence(tokens.map(token => {
          val tags = token.split('|')
          Token.put(tags.dropRight(1))
        }))
      })

      testArrayWithLabelOpt = testData.map(sentence => {
        val tokens = sentence.split("\t")
        Sequence(tokens.map(token => {
          val tags = token.split('|')
          Token.put(tags.last, tags.dropRight(1))
        }))
      })

      val model = new CRF().setRegParam(regParam)
        .setFreq(frequency)
        .setMaxIterations(maxIteration)
        .setTolerance(tol)
        .setRegularization(L2)
        .setCompLevel(compLevel)
        .setCalcAccuracy(calAcc)
        .setNumThread(nThread)
        .runCRF(templates, trainRDDOpt, testArrayWithLabelOpt, testArrayWithoutLabelOpt)

      val time1 = System.currentTimeMillis()
      val testDataWithLabelOpt = sc.textFile(dataPath + ".t").filter(_.nonEmpty).map { sentence =>
        val tokens = sentence.split("\t")
        Sequence(tokens.map { token =>
          val tags: Array[String] = token.split('|')
          Token.put(tags.dropRight(1))
        })
      }

      val testDataWithoutLabelOpt = sc.textFile(dataPath + ".t").filter(_.nonEmpty).map { sentence =>
        val tokens = sentence.split("\t")
        Sequence(tokens.map { token =>
          val tags: Array[String] = token.split('|')
          Token.put(tags.last, tags.dropRight(1))
        })
      }

      val results = model.predict(testDataWithoutLabelOpt)
      val score = results
        .zipWithIndex()
        .map(_.swap)
        .join(testDataWithLabelOpt.zipWithIndex().map(_.swap))
        .map(_._2)
        .map(x => x._1.compare(x._2))
        .reduce(_ + _)
      val total = testDataWithoutLabelOpt.map(_.toArray.length).reduce(_ + _)

      val acc = score / total.toDouble
      Utils.saveEvaluation(acc, params.saveDataPath, sc)
      val time2 = System.currentTimeMillis()
      predictTime += (time2 - time1).toDouble / 1000
    } else {
      println("================== Running Raw CRF ==================")

      val trainDataRaw = sc.textFile(dataPath + ".tr").filter(_.nonEmpty).map { sentence =>
        val tokens = sentence.split("\t")
        Seq(tokens.map { token =>
          val tags: Array[String] = token.split('|')
          Tok.put(tags.last, tags.dropRight(1))
        })
      }

      var testArrayWithoutLabelRaw: Array[Seq] = Array[Seq]()
      var testArrayWithLabelRaw: Array[Seq] = Array[Seq]()

      val trainRDDRaw = sc.parallelize(trainDataRaw.collect(), pt).repartition(pt).cache()
      //println("number of training sequences: " + trainRDDRaw.count())

      testArrayWithoutLabelRaw = testData.map(sentence => {
        val tokens = sentence.split("\t")
        Seq(tokens.map(token => {
          val tags = token.split('|')
          Tok.put(tags.dropRight(1))
        }))
      })

      testArrayWithLabelRaw = testData.map(sentence => {
        val tokens = sentence.split("\t")
        Seq(tokens.map(token => {
          val tags = token.split('|')
          Tok.put(tags.last, tags.dropRight(1))
        }))
      })

      val model = new CRFRaw().setRegParam(regParam)
        .setFreq(frequency)
        .setMaxIterations(maxIteration)
        .setEta(tol)
        .setRegularization(L2Raw)
        .setCalcAccuracy(calAcc)
        .runCRF(templates, trainRDDRaw, testArrayWithLabelRaw, testArrayWithoutLabelRaw)

      val time1 = System.currentTimeMillis()
      val testDataWithLabelRaw = sc.textFile(dataPath + ".t").filter(_.nonEmpty).map { sentence =>
        val tokens = sentence.split("\t")
        Seq(tokens.map { token =>
          val tags: Array[String] = token.split('|')
          Tok.put(tags.dropRight(1))
        })
      }

      val testDataWithoutLabelRaw = sc.textFile(dataPath + ".t").filter(_.nonEmpty).map { sentence =>
        val tokens = sentence.split("\t")
        Seq(tokens.map { token =>
          val tags: Array[String] = token.split('|')
          Tok.put(tags.last, tags.dropRight(1))
        })
      }

      val results = model.predict(testDataWithoutLabelRaw)
      val score = results
        .zipWithIndex()
        .map(_.swap)
        .join(testDataWithLabelRaw.zipWithIndex().map(_.swap))
        .map(_._2)
        .map(x => x._1.compare(x._2))
        .reduce(_ + _)

      val total = testDataWithoutLabelRaw.map(_.toArray.length).reduce(_ + _)

      val acc = score / total.toDouble
      Utils.saveEvaluation(acc, params.saveDataPath, sc)
      val time2 = System.currentTimeMillis()
      predictTime += (time2 - time1).toDouble / 1000
    }
    val t3 = System.currentTimeMillis()
    println("* train time = %f[s]", (t3 - t2) / 1000.0)
    println("\n--------success--------\n")

    val totalTaining = (t3 - t1).toDouble / 1000 - predictTime
    val coreTraining = (t3 - t2).toDouble / 1000 - predictTime
    val dataLoading = (t2 - t1).toDouble / 1000
    println("[s]end2end train:   " + totalTaining)
    println("[s]data preprocess: " + dataLoading)
    println("[s]core train:      " + coreTraining)

    totalTaining
  }
}