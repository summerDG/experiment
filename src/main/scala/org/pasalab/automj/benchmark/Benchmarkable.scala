package org.pasalab.automj.benchmark

/**
 * Created by wuxiaoqi on 18-1-3.
 */
import java.util.UUID

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.{MjSession, SQLContext, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._


/** A trait to describe things that can be benchmarked. */
abstract class Benchmarkable(@transient val mjSession: MjSession) extends LazyLogging {
  @transient protected[this] val sparkContext = mjSession.sparkContext

  val name: String
  protected val executionMode: ExecutionMode

  final def benchmark(
                       includeBreakdown: Boolean,
                       description: String = "",
                       messages: ArrayBuffer[String],
                       timeout: Long,
                       forkThread: Boolean = true): BenchmarkResult = {
    logger.info(s"$this: benchmark")
    sparkContext.setJobDescription(s"Execution: $name, $description")
    beforeBenchmark()
    val result = if (forkThread) {
      runBenchmarkForked(includeBreakdown, description, messages, timeout)
    } else {
      doBenchmark(includeBreakdown, description, messages)
    }
    afterBenchmark(mjSession.sparkContext)
    result
  }

  protected def beforeBenchmark(): Unit = { }

  protected def afterBenchmark(sc: SparkContext): Unit = {
    System.gc()
  }

  private def runBenchmarkForked(
                                  includeBreakdown: Boolean,
                                  description: String = "",
                                  messages: ArrayBuffer[String],
                                  timeout: Long): BenchmarkResult = {
    val jobgroup = UUID.randomUUID().toString
    val that = this
    var result: BenchmarkResult = null
    val thread = new Thread("benchmark runner") {
      override def run(): Unit = {
        logger.info(s"$that running $this")
        sparkContext.setJobGroup(jobgroup, s"benchmark $name", true)
        try {
          result = doBenchmark(includeBreakdown, description, messages)
        } catch {
          case e: Throwable =>
            logger.info(s"$that: failure in runBenchmark: $e")
            println(s"$that: failure in runBenchmark: $e")
            result = BenchmarkResult(
              name = name,
              mode = executionMode.toString,
              parameters = Map.empty,
              failure = Some(Failure(e.getClass.getSimpleName,
                e.getMessage + ":\n" + e.getStackTraceString)))
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
    thread.join(timeout)
    if (thread.isAlive) {
      sparkContext.cancelJobGroup(jobgroup)
      thread.interrupt()
      result = BenchmarkResult(
        name = name,
        mode = executionMode.toString,
        failure = Some(Failure("Timeout", s"timeout after ${timeout / 1000} seconds"))
      )
    }
    result
  }

  protected def doBenchmark(
                             includeBreakdown: Boolean,
                             description: String = "",
                             messages: ArrayBuffer[String]): BenchmarkResult

  protected def measureTimeMs[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }

  protected def measureTime[A](f: => A): (Duration, A) = {
    val startTime = System.nanoTime()
    val res = f
    val endTime = System.nanoTime()
    (endTime - startTime).nanos -> res
  }
}
