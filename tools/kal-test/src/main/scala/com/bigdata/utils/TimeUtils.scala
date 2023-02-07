package com.bigdata.utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TimeUtils {
  val NANO_TO_SEC = 1000000000.0

  class Timer (name: String = "Perf") {
    val ts = mutable.Map[String, ArrayBuffer[Long]]()
    var prev: Long = 0L
    var reportOrder = ArrayBuffer[String]()
    var startTime = prev

    def start(): this.type = {
      prev = System.nanoTime()
      startTime = prev
      this
    }

    def tic(tag: String): Unit = {
      if (!reportOrder.contains(tag)) {reportOrder.append(tag)}
      val curr = System.nanoTime()
      if (ts.contains(tag)) {
        ts(tag).append(curr - prev)
      } else { ts.update(tag, ArrayBuffer(curr - prev)) }
      prev = curr }

    def report(): Unit = {
      for (tag <- reportOrder) {
        println(s"[${name}] ${tag}: ${ts(tag).sum / NANO_TO_SEC} s")
      }
      println(s"[${name}] Total: ${(prev - startTime) / NANO_TO_SEC}")
    }

    def getElapsedTime(): Double = { (prev - startTime) / NANO_TO_SEC }
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("*******************************")
    println("Elapsed time: " + (t1 - t0) * 1.0 / 1000000000 + "s")
    println("*******************************")
    result
  }
}
