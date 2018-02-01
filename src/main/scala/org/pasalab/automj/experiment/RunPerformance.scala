package org.pasalab.automj.experiment

import java.io.File

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, MjSession, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.pasalab.automj.MjConfigConst
import org.pasalab.automj.benchmark.{ExperimentConst, Table}

import scala.io.Source

object RunPerformance {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.2.0")
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
        .required()
      opt[String]('t', "tablesFile")
        .action { (x, c) => c.copy(tablesFile = x) }
        .text("the tables of the benchmark, include name and path")
      opt[String]('q', "queriesFile")
        .action { (x, c) => c.copy(queriesFile = x) }
        .text("the queries of the benchmark, include name and sql text")
      opt[String]('C', "configFile")
        .action { (x, c) => c.copy(configFile = x) }
        .text("the configuration of the benchmark")
      opt[String]('f', "filter")
        .action((x, c) => c.copy(filter = Some(x)))
        .text("a filter on the name of the queries to run")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[Long]('c', "compare")
        .action((x, c) => c.copy(baseline = Some(x)))
        .text("the timestamp of the baseline experiment to compare with")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }
  def run(config: RunConfig): Unit ={
    val conf = {
      val configuration = new SparkConf()
        .setAppName(getClass.getName)
      // 设置配置文件里的参数
      Source.fromFile(config.configFile).getLines().foreach {
        case line =>
          val pair = line.split("\\s+")
          assert(pair.length == 2, s"please use correct file format($line), <config name> <value>")
          configuration.set(pair(0), pair(1))
      }
      configuration.set(ExperimentConst.TABLES_FILE, config.tablesFile)
      configuration.set(ExperimentConst.QUERIES_FILE, config.queriesFile)
      configuration.set(ExperimentConst.TABLES_FILE, config.tablesFile)
      configuration
    }

    val sc = SparkContext.getOrCreate(conf)
    val spark = new MjSession(sc)
    val sqlContext = spark.sqlContext

    sqlContext.setConf("spark.sql.perf.results", new java.io.File("performance").toURI.toString)
    sqlContext.setConf(MjConfigConst.JOIN_DEFAULT_SIZE, sc.getConf.get(MjConfigConst.JOIN_DEFAULT_SIZE))
    sqlContext.setConf(MjConfigConst.ONE_ROUND_PARTITIONS, sc.getConf.get(MjConfigConst.ONE_ROUND_PARTITIONS))

//    val rdd = sc.parallelize(1 to 100, 4).flatMap(s => (1 to 100).map(t => (s, t))).map {
//      case x => Row.fromTuple(x)
//    }
//    val df1 = spark.createDataFrame(rdd, StructType(Seq(StructField("x", IntegerType), StructField("z", IntegerType))))
//    val df2 = spark.createDataFrame(rdd, StructType(Seq(StructField("x", IntegerType), StructField("y", IntegerType))))
//    val df3 = spark.createDataFrame(rdd, StructType(Seq(StructField("y", IntegerType), StructField("z", IntegerType))))
//    df1.createOrReplaceTempView("a")
//    df2.createOrReplaceTempView("b")
//    df3.createOrReplaceTempView("c")
    val tablesFile = sc.getConf.get(ExperimentConst.TABLES_FILE)
    assert(new File(tablesFile).exists(), s"file <${tablesFile}> not exist")
    val names: Seq[(String, String)] = Source.fromFile(tablesFile).getLines().map {
      case line =>
        val pair = line.split("\\s+")
        assert(pair.length == 2, s"please use correct file format($line), <name> <path>")
        (pair(0), pair(1))
    }.toSeq

    val dfs: Seq[(String, DataFrame)] = names.map {
      case (name, path) =>
        (name, spark.read.json(path))
    }
    dfs.foreach {
      case (name, df) =>
        // 这里必须先注册, 否则后续的sql会找不到表名
        df.createOrReplaceTempView(name)
    }

    val queriesFile = sc.getConf.get(ExperimentConst.QUERIES_FILE)
    assert(new File(queriesFile).exists(), s"file <${queriesFile}> not exist")
    val sqlText: Seq[(String, String)] = Source.fromFile(queriesFile).getLines().map {
      case line =>
        val i = line.indexOf("\t")
        val name = line.substring(0, i)
        val sqlText = line.substring(i + 1)
        (name, sqlText)
    }.toSeq


    (0 until config.iterations).foreach {
      case i =>
        println(s"============================iteration $i==============================")
        execute(spark, sqlText, "one-round")
        execute(spark, sqlText, "mixed")
    }
  }
  def execute(spark:MjSession, allQueries: Seq[(String, String)], mode: String): Unit ={
    spark.sqlContext.setConf(MjConfigConst.EXECUTION_MODE, mode)
    println(s"execution mode: $mode")
    allQueries.foreach {
      case (name:String, sql: String) =>
        spark.sqlContext.setConf(MjConfigConst.ONE_ROUND_ONCE, "true")
        println(s"query: ${name}")
        if (name == "arbitrary" && mode == "one-round") {
          val time = measureTimeMs(spark.sql(sql).queryExecution.optimizedPlan)
          println(s"optimized time: $time")
        } else {
          val time = measureTimeMs(spark.sql(sql).queryExecution.toRdd.foreach(_ => Unit))
          println(s"execution time: $time")
        }
    }
  }
  def measureTimeMs[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }
}
