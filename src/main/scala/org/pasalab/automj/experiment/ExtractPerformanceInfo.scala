package org.pasalab.automj.experiment

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pasalab.automj.benchmark.ExperimentRun

object ExtractPerformanceInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    val sqlContext = spark.sqlContext
    import spark.implicits._
    val df = spark.read
      .schema(ScalaReflection.schemaFor[ExperimentRun].dataType.asInstanceOf[StructType])
      .json(new java.io.File("performance").toURI.toString+"/timestamp="+args(0))
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
    showResult(df
      .where($"tags".getItem("Mj execution mode") === "default"))
    println("== One Round Strategy ==")
    showResult(df
      .where($"tags".getItem("Mj execution mode") === "one-round"))
    println("== Mixed(one-round & multi-round) Strategy ==")
    showResult(df
      .where($"tags".getItem("Mj execution mode") === "mixed"))

  }
}
