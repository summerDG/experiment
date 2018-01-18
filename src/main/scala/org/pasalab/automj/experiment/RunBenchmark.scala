package org.pasalab.automj.experiment

import org.apache.spark.sql.{DataFrame, MjSession, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.{SparkConf, SparkContext}
import org.pasalab.automj.MjConfigConst
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
    import spark.implicits._

    sqlContext.setConf("spark.sql.perf.results", new java.io.File("performance").toURI.toString)
//    sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
//    sqlContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    sqlContext.setConf(MjConfigConst.JOIN_DEFAULT_SIZE, sc.getConf.get(MjConfigConst.JOIN_DEFAULT_SIZE))
    sqlContext.setConf(MjConfigConst.ONE_ROUND_PARTITIONS, sc.getConf.get(MjConfigConst.ONE_ROUND_PARTITIONS))

    val benchmark = Try {
      Class.forName(config.benchmarkName)
        .getConstructors.head.newInstance(spark).asInstanceOf[Benchmark]
    } getOrElse {
      Class.forName("org.pasalab.automj.benchmark." + config.benchmarkName)
        .getConstructors.head.newInstance(spark).asInstanceOf[Benchmark]
    }

    benchmark.allTables
    val allQueries = config.filter.map { f =>
      benchmark.allQueries.filter(_.name contains f)
    } getOrElse {
      assert(benchmark.allQueries.nonEmpty, s"no queries")
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
        "host" -> conf.getOption("spark.master").get), forkThread = false)

    println("== STARTING EXPERIMENT ==")
    experiment.waitForFinish(1000 * 60 * 30)
    def showResult(df: DataFrame): Unit ={
      df.withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"executionTime") as 'minExeTimeMs,
          max($"executionTime") as 'maxExeTimeMs,
          avg($"executionTime") as 'avgExeTimeMs,
          stddev($"executionTime") as 'exeStdDev,
          min($"optimizationTime") as 'minOptTimeMs,
          max($"optimizationTime") as 'maxOptTimeMs,
          avg($"optimizationTime") as 'avgOptTimeMs,
          stddev($"optimizationTime") as 'optStdDev)
        .orderBy("name")
        .show(truncate = false)
    }

    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    println("== Spark Default Strategy ==")
    showResult(experiment.getCurrentRuns()
      .where($"tags".getItem("Mj execution mode") === "default"))
    println("== One Round Strategy ==")
    showResult(experiment.getCurrentRuns()
      .where($"tags".getItem("Mj execution mode") === "one-round"))
    println("== Mixed(one-round & multi-round) Strategy ==")
    showResult(experiment.getCurrentRuns()
      .where($"tags".getItem("Mj execution mode") === "mixed"))

    println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")

    config.baseline.foreach { baseTimestamp =>
      val baselineTime = when($"timestamp" === baseTimestamp, $"executionTime").otherwise(null)
      val thisRunTime = when($"timestamp" === experiment.timestamp, $"executionTime").otherwise(null)

      val data = spark.read.json(benchmark.resultsLocation)
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
