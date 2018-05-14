package org.pasalab.automj.experiment

import org.apache.spark.sql.SparkSession
import org.pasalab.automj.MjConfigConst

import scala.io.Source

object JoinSizeEstimation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    assert(args.length == 3, s"JoinSizeEstimation <Tables File> <Query File> <Probability>")
    val tablesFile = args(0)
    val queryFile = args(1)
    val p = args(2).toDouble
    for (line <- Source.fromFile(tablesFile).getLines()) {
      val pairs = line.split("\\s+")
      assert(pairs.length >= 2, s"File format error: <Table Name> <Path>")
      val name = pairs(0)
      val path = pairs(1)
      val df = if (!name.contains("pubCi")) {
        spark.read.json(path).sample(false, p)
      } else {
        spark.read.json(path)
      }
      df.createTempView(name)
      println(s"Name: $name, Sample Count: ${df.count()}")
    }
    for (line <- Source.fromFile(queryFile).getLines()) {
      val pairs = line.split("\t")
      assert(pairs.length >= 4, s"File format error: <Query Name> <SQL> <Tables Count> <default/one-round>")
      spark.sqlContext.setConf(MjConfigConst.EXECUTION_MODE, pairs(3))
      val name = pairs(0)
      val sql = pairs(1)
      val c = pairs(2).toInt
      val df = spark.sql(sql)
      df.createTempView(name)
      val queryExecution = df.queryExecution
      queryExecution.executedPlan
      println(s"Query: $name, EstimationCount: ${queryExecution.toRdd.count()/math.pow(p, c)}, mode: ${pairs(3)}")
      val executionTime = measureTimeMs(queryExecution.toRdd.foreach(_ => Unit))
      println(s"execution time: $executionTime")
    }
  }
  def measureTimeMs[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }
}
