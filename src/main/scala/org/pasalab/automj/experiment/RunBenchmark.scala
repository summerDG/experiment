package org.pasalab.automj.experiment

import org.apache.spark.sql.MjSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.pasalab.automj.benchmark.{Benchmark, ExperimentConst}

import scala.io.Source
import scala.util.Try

case class RunConfig(
                      benchmarkName: String = null,
                      filter: Option[String] = None,
                      tablesFile: String = ExperimentConst.DEFAULT_TABLES_FILE,
                      queriesFile: String = ExperimentConst.DEFAULT_QUERIES_FILE,
                      configFile: String = ExperimentConst.DEFAULT_CONFIG_FILE,
                      iterations: Int = 3,
                      baseline: Option[Long] = None)

/**
 * Runs a benchmark locally and prints the results to the screen.
 */
object RunBenchmark {
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

  def run(config: RunConfig): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)

    // 设置配置文件里的参数
    Source.fromFile(config.configFile).getLines().map {
      case line =>
        val pair = line.split("\\s+")
        assert(pair.length == 2, s"please use correct file format($line), <config name> <value>")
        conf.set(pair(0), pair(1))
    }

    val sc = SparkContext.getOrCreate(conf)
    val spark = new MjSession(sc)
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    sqlContext.setConf("spark.sql.perf.results", new java.io.File("performance").toURI.toString)
    sqlContext.setConf(ExperimentConst.TABLES_FILE, config.tablesFile)
    sqlContext.setConf(ExperimentConst.QUERIES_FILE, config.queriesFile)
    sqlContext.setConf(ExperimentConst.CONFIG_FILE, config.configFile)

    val benchmark = Try {
      Class.forName(config.benchmarkName)
        .newInstance()
        .asInstanceOf[Benchmark]
    } getOrElse {
      Class.forName("com.databricks.spark.sql.perf." + config.benchmarkName)
        .newInstance()
        .asInstanceOf[Benchmark]
    }

    val allQueries = config.filter.map { f =>
      benchmark.allQueries.filter(_.name contains f)
    } getOrElse {
      benchmark.allQueries
    }

    println("== QUERY LIST ==")
    allQueries.foreach(println)

    val experiment = benchmark.runExperiment(
      executionsToRun = allQueries,
      iterations = config.iterations,
      variations = Seq(benchmark.executionMode),
      tags = Map(
        "runtype" -> "cluster",
        "host" -> conf.getOption("spark.master").get))

    println("== STARTING EXPERIMENT ==")
    experiment.waitForFinish(1000 * 60 * 30)

    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    experiment.getCurrentRuns()
      .withColumn("result", explode($"results"))
      .select("result.*")
      .groupBy("name")
      .agg(
        min($"executionTime") as 'minTimeMs,
        max($"executionTime") as 'maxTimeMs,
        avg($"executionTime") as 'avgTimeMs,
        stddev($"executionTime") as 'stdDev)
      .orderBy("name")
      .show(truncate = false)
    println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")

    config.baseline.foreach { baseTimestamp =>
      val baselineTime = when($"timestamp" === baseTimestamp, $"executionTime").otherwise(null)
      val thisRunTime = when($"timestamp" === experiment.timestamp, $"executionTime").otherwise(null)

      val data = sqlContext.read.json(benchmark.resultsLocation)
        .coalesce(1)
        .where(s"timestamp IN ($baseTimestamp, ${experiment.timestamp})")
        .withColumn("result", explode($"results"))
        .select("timestamp", "result.*")
        .groupBy("name")
        .agg(
          avg(baselineTime) as 'baselineTimeMs,
          avg(thisRunTime) as 'thisRunTimeMs,
          stddev(baselineTime) as 'stddev)
        .withColumn(
          "percentChange", ($"baselineTimeMs" - $"thisRunTimeMs") / $"baselineTimeMs" * 100)
        .filter('thisRunTimeMs.isNotNull)

      data.show(truncate = false)
    }
  }
}
