package org.pasalab.automj.experiment

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{MjSession, SparkSession}
import org.pasalab.automj.MjConfigConst
import org.pasalab.automj.experiment.RunBenchmark.getClass
import org.pasalab.automj.experiment.TwitterQueryCount.getClass

import scala.io.Source

object DoubleTriangle {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
    assert(args.length == 2, s"JoinSizeEvaluation <Tables File>")
    val configFile = args(1)
    Source.fromFile(configFile).getLines().foreach {
      case line =>
        val pair = line.split("\\s+")
        assert(pair.length == 2, s"please use correct file format($line), <config name> <value>")
        conf.set(pair(0), pair(1))
    }
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._


    val tablesFile = args(0)
    for (line <- Source.fromFile(tablesFile).getLines()) {
      val pairs = line.split("\\s+")
      assert(pairs.length >= 2, s"File format error: <Table Name> <Path>")
      val name = pairs(0)
      val path = pairs(1)
      val df = spark.read.json(path)
      df.createTempView(name)
      println(s"Name: $name, Count: ${df.count()}")
    }

    conf.set(MjConfigConst.EXECUTION_MODE, "one-round")
    val abe = spark.sql("SELECT * FROM a, b, e WHERE a.y = b.y AND b.z = e.z AND e.x = a.x")
    val gij = spark.sql("SELECT * FROM g, i, j WHERE g.q = i.q AND i.r = j.r AND j.p = g.p")

    val queryExecution1 = abe.queryExecution
    val queryExecution2 = gij.queryExecution
    spark.sqlContext.setConf(MjConfigConst.ONE_ROUND_ONCE, "true")
    val executionPlan1 = measureTimeMs(queryExecution1.executedPlan)
    val time1 = measureTimeMs(queryExecution1.toRdd.foreach(_ => Unit))
    spark.sqlContext.setConf(MjConfigConst.ONE_ROUND_ONCE, "true")
    val executionPlan2 = measureTimeMs(queryExecution2.executedPlan)
    val time2 = measureTimeMs(queryExecution2.toRdd.foreach(_ => Unit))
    val df1 = abe.cache()
    val df2 = gij.cache()
    println(s"<<abe>> opt time: $executionPlan1, exe time: $time1")
    println(s"<<gij>> opt time: $executionPlan2, exe time: $time2")
    conf.set(MjConfigConst.EXECUTION_MODE, "default")
    val f = spark.read.json("wuxiaoqi/tables/f")
    val df = df1.join(f.as("connect"), $"e.z" === $"connect.z").join(df2, $"connect.p" === $"g.p")
    val queryExecution3 = df.queryExecution
    val executionPlan3 = measureTimeMs(queryExecution3.executedPlan)
    val time3 = measureTimeMs(queryExecution3.toRdd.foreach(_ => Unit))
    println(s"<<q3>> opt time: $executionPlan3, exe time: $time3")
    while (true) {
    }
  }
  def measureTimeMs[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }
}
