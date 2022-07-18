package com.bigdata.ml

import com.bigdata.compare.ml.Word2VecEvaluation

import java.io.{File, FileWriter}
import java.util
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty
import com.bigdata.utils.Utils
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.mllib.feature.{Word2VecModel => mllibWord2VecModel}

class Word2VecConfig extends Serializable {
  @BeanProperty var word2vec: util.HashMap[String, util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]]] = _
}

class Word2VecParams extends Serializable {
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var vectorSize: Int = _
  @BeanProperty var minCount: Int = _
  @BeanProperty var window: Int = _
  @BeanProperty var numIterations: Int = _
  @BeanProperty var learningRate: Double = _
  @BeanProperty var regularization: Double = _
  @BeanProperty var repetition: Int = _
  @BeanProperty var eval: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var metric: Double = _

  @BeanProperty var sentenceFile: String = _
  @BeanProperty var downstreamTrainFile: String = _
  @BeanProperty var downstreamTestFile: String = _
  @BeanProperty var fieldName: String = _
  @BeanProperty var isRaw: String = _
}

object Word2VecRunner {
  def main(args: Array[String]): Unit = {
    try {
      val sentenceFile = args(0)
      val downstreamTrainFile = args(1)
      val downstreamTestFile = args(2)
      val dataSet = args(3).split("-")
      val (fieldName, apiName, scalaVersion) = (dataSet(0), dataSet(1), dataSet(2))
      val isRaw = args(4)
      val sparkConfSplit = args(5).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))

      val stream = Utils.getStream("conf/ml/word2vec/word2vec.yml")
      val typeRaw = isRaw match {
        case "no" => "opt"
        case "yes" => "raw"
      }

      val representer = new Representer
      representer.addClassTag(classOf[Word2VecParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[Word2VecConfig]), representer, options)
      val description = new TypeDescription(classOf[Word2VecParams])
      yaml.addTypeDescription(description)

      val configs: Word2VecConfig = yaml.load(stream).asInstanceOf[Word2VecConfig]
      val params = new Word2VecParams()
      val paramsMap: util.HashMap[String, Object] = configs.word2vec.get(typeRaw).get("scala" + scalaVersion).get(fieldName)

      params.setSentenceFile(sentenceFile)
      params.setDownstreamTrainFile(downstreamTrainFile)
      params.setDownstreamTestFile(downstreamTestFile)
      params.setFieldName(fieldName)
      params.setIsRaw(isRaw)
      params.setApiName(apiName)
      params.setNumPartitions(paramsMap.get("numPartitions").asInstanceOf[Int])
      params.setVectorSize(paramsMap.get("vectorSize").asInstanceOf[Int])
      params.setMinCount(paramsMap.get("minCount").asInstanceOf[Int])
      params.setWindow(paramsMap.get("window").asInstanceOf[Int])
      params.setNumIterations(paramsMap.get("numIterations").asInstanceOf[Int])
      params.setLearningRate(paramsMap.get("learningRate").asInstanceOf[Double])
      params.setEval(paramsMap.get("eval").asInstanceOf[String])
      val conf = new SparkConf().setAppName(s"word2vec_${fieldName}_${apiName}").setMaster(master)
      val commonParas = Array (
        ("spark.submit.deployMode", deployMode),
        ("spark.executor.instances", numExec),
        ("spark.executor.cores", execCores),
        ("spark.executor.memory", execMem)
      )
      conf.setAll(commonParas)

      if ("no" == isRaw.asInstanceOf[String]) {
        params.setRegularization(paramsMap.get("regularization").asInstanceOf[Double])
        params.setRepetition(paramsMap.get("repetition").asInstanceOf[Int])
        conf.set("spark.boostkit.mllib.feature.word2vec.regularization", params.regularization.toString)
        conf.set("spark.boostkit.mllib.feature.word2vec.repetition", params.repetition.toString)
      }
      val sc = new SparkContext(conf)

      val startTime = System.currentTimeMillis()

      val spark = SparkSession.builder.config(conf).getOrCreate()
      import spark.implicits._
      val sentences = sc.objectFile[Array[String]](sentenceFile, params.numPartitions)
        .map(_.toSeq)
        .cache()
        .setName("ori")
        .toDF("sentences")
      println(s"********** read data, sentenceCnt=${sentences.count()}, time=${(System.currentTimeMillis() - startTime) / 60000.0} **********")

      val exeTime = System.currentTimeMillis()
      val w2v = new Word2Vec()
        .setInputCol("sentences")
        .setNumPartitions(params.numPartitions)
        .setWindowSize(params.window)
        .setStepSize(params.learningRate)
        .setVectorSize(params.vectorSize)
        .setMaxIter(params.numIterations)

      val paramMap = ParamMap(w2v.minCount -> params.minCount)
        .put(w2v.windowSize, params.window)

      val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
      for (i <- 0 to paramMaps.size -1) {
        paramMaps(i) = ParamMap(w2v.minCount -> params.minCount)
          .put(w2v.windowSize, params.window)
      }

      val learnParamPair = ParamPair(w2v.stepSize, params.learningRate)
      val maxIterParamPair = ParamPair(w2v.maxIter, params.numIterations)
      val maxVectorParamPair = ParamPair(w2v.vectorSize, params.vectorSize)

      val model = params.apiName match {
        case "fit1" => w2v.fit(sentences, paramMap)
        case "fit2" =>
          val models = w2v.fit(sentences, paramMaps)
          models(0)
        case "fit3" => w2v.fit(sentences, learnParamPair, maxIterParamPair, maxVectorParamPair)
        case _ => w2v.fit(sentences)
      }
      println(s"********** train time=${(System.currentTimeMillis() - exeTime) / 60000.0} **********")

      val ju = scala.reflect.runtime.universe
      val mirror = ju.runtimeMirror(model.getClass.getClassLoader)
      val instanceMirror = mirror.reflect(model)
      val myMethod = ju.typeOf[Word2VecModel].decl(ju.TermName("wordVectors")).asMethod
      val myFunc = instanceMirror.reflectMethod(myMethod)
      val mllibModel = myFunc().asInstanceOf[mllibWord2VecModel]

      val metric = params.eval match {
        case "taobao" => Word2VecEvaluation.evaluateTaobaoProductCTR(spark, downstreamTrainFile, downstreamTestFile, mllibModel, params.numPartitions)
        case "alibaba" => Word2VecEvaluation.evaluateAlibabaCTR(spark, Array(fieldName), downstreamTrainFile, downstreamTestFile, Array(mllibModel), params.numPartitions)
      }
      val costTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"********** metric=${metric} **********")

      params.setCostTime(costTime)
      params.setMetric(metric)

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/Word2Vec_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")

      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
