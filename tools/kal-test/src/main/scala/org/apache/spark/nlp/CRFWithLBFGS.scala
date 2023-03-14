// scalastyle:off
package org.apache.spark.nlp

import scala.collection.mutable
import breeze.optimize.{CachedDiffFunction, DiffFunction, OWLQN => BreezeOWLQN, LBFGS => BreezeLBFGS}
import breeze.linalg.{DenseVector => BDV, sum => Bsum}
import org.apache.spark.rdd.RDD
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.linalg.{Vector => SparkVector}


class CRFWithLBFGS(private var gradient: CRFGradient, private var updater: Updater)
  extends LBFGS(gradient: Gradient, updater: Updater) {

  private val numCorrections = 5
  private var maxNumIterations = 100
  private var convergenceTol = 1E-4
  private var regParam = 0.5

  /**
   * Set the regularization parameter. Default 0.5.
   */
  override def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the convergence tolerance of iterations for L-BFGS. Default 1E-4.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * This value must be nonnegative. Lower convergence values are less tolerant
   * and therefore generally cause more iterations to be run.
   */
  override def setConvergenceTol(tolerance: Double): this.type = {
    this.convergenceTol = tolerance
    this
  }

  /**
   * Set the maximal number of iterations for L-BFGS. Default 100.
   */
  override def setNumIterations(iters: Int): this.type = {
    this.maxNumIterations = iters
    this
  }

  def optimizer(data: RDD[Tagger], initialWeights: BDV[Double]): BDV[Double] = {
    CRFWithLBFGS.runLBFGS(data,
      gradient,
      updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeights)
  }
}

object CRFWithLBFGS extends Logging {
  def runLBFGS(
      data: RDD[Tagger],
      gradient: CRFGradient,
      updater: Updater,
      numCorrections: Int,
      convergenceTol: Double,
      maxNumIterations: Int,
      regParam: Double,
      initialWeights: BDV[Double]): BDV[Double] = {

    val costFun = new CostFun(data, gradient, updater, regParam)

    var lbfgs: BreezeLBFGS[BDV[Double]] = null

    updater match {
      case updater: L1Updater =>
        lbfgs = new BreezeOWLQN[Int, BDV[Double]](maxNumIterations, numCorrections, regParam, convergenceTol)
      case updater: L2Updater =>
        lbfgs = new BreezeLBFGS[BDV[Double]](maxNumIterations, numCorrections, convergenceTol)
    }

    val states = lbfgs.iterations(new CachedDiffFunction[BDV[Double]](costFun), initialWeights)

    val lossHistory = mutable.ArrayBuilder.make[Double]
    var state = states.next()
    while (states.hasNext) {
      lossHistory += state.value
      state = states.next()
    }

    logInfo("LBFGS.runLBFGS finished after %s iterations. last 10 losses: %s".format(
      state.iter, lossHistory.result().takeRight(10).mkString(" -> ")))
    state.x
  }
}

class CRFGradient extends Gradient {
  def compute(
      data: SparkVector,
      label: Double,
      weights: SparkVector,
      cumGradient: SparkVector): Double = {
    throw new Exception("The original compute() method is not supported")
  }

  def computeCRF(sentences: Iterator[Tagger], weights: BDV[Double]): (BDV[Double], Double) = {

    val expected = BDV.zeros[Double](weights.length)
    var obj: Double = 0.0
    while (sentences.hasNext)
      obj += sentences.next().gradient(expected, weights)

    (expected, obj)
  }
}

trait UpdaterCRF extends Updater {
  def compute(
      weightsOld: SparkVector,
      gradient: SparkVector,
      stepSize: Double,
      iter: Int,
      regParam: Double) = {
    throw new Exception("The original compute() method is not supported")
  }
  def computeCRF(weightsOld: BDV[Double], gradient: BDV[Double], regParam: Double): (BDV[Double], Double)
}

class L2Updater extends UpdaterCRF {
  def computeCRF(
      weightsOld: BDV[Double],
      gradient: BDV[Double],
      regParam: Double): (BDV[Double], Double) = {
    val loss = Bsum(weightsOld *:* weightsOld *:* regParam)
    gradient :+= weightsOld *:* (regParam * 2.0)
    (gradient, loss)
  }
}

class L1Updater extends UpdaterCRF {
  def computeCRF(
                  weightsOld: BDV[Double],
                  gradient: BDV[Double],
                  regParam: Double): (BDV[Double], Double) = {
    (gradient, 0.0)
  }
}

private class CostFun(
    taggers: RDD[Tagger],
    gradient: CRFGradient,
    updater: Updater,
    regParam: Double) extends DiffFunction[BDV[Double]] with Logging with Serializable {

  var iter = 0

  override def calculate(weigths: BDV[Double]): (Double, BDV[Double]) = {
    val start = System.currentTimeMillis
    val bcWeights = taggers.context.broadcast(weigths)
    lazy val treeDepth = math.ceil(math.log(taggers.partitions.length) / (math.log(2) * 2)).toInt

    val (expected, obj) = taggers.mapPartitions(sentences =>
      Iterator(gradient.computeCRF(sentences, bcWeights.value))
    ).treeReduce((p1, p2) => (p1, p2) match {
      case ((expected1, obj1), (expected2, obj2)) =>
        (expected1 + expected2, obj1 + obj2)
    }, treeDepth)

    val (grad, loss) = updater.asInstanceOf[UpdaterCRF].computeCRF(weigths, expected, regParam)
    val end = System.currentTimeMillis

    logInfo(s"iter_$iter, Run Time = ${(end - start) / 1000.0}[s]\n")
    iter += 1
    (obj + loss, grad)
  }
}

