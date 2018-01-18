package org.pasalab.automj.benchmark

import java.io.File

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, MjSession, Row, SparkSession}
import org.pasalab.automj.benchmark.ExecutionMode.{CollectResults, ForeachResults}

import scala.io.Source

/**
 * Created by wuxiaoqi on 18-1-5.
 */
class AutoMjPerformance(mjSession: MjSession) extends Benchmark(mjSession) {

  val joinTables: Seq[Table] = {
    val tablesFile = currentConfiguration.sparkConf.get(ExperimentConst.TABLES_FILE).getOrElse("")
    assert(new File(tablesFile).exists(), s"file <${tablesFile}> not exist")
    val names: Seq[(String, String)] = Source.fromFile(tablesFile).getLines().map {
      case line =>
        val pair = line.split("\\s+")
        assert(pair.length == 2, s"please use correct file format($line), <name> <path>")
        (pair(0), pair(1))
    }.toSeq

    val dfs: Seq[(String, DataFrame)] = names.map {
      case (name, path) =>
        (name, mjSession.read.json(path))
    }
    dfs.map {
      case (name, df) =>
        // 这里必须先注册, 否则后续的sql会找不到表名
        df.createOrReplaceTempView(name)
        Table(name, df)
    }
//    val sc = mjSession.sparkContext
//    val rdd = sc.parallelize(1 to 100, 4).flatMap(s => (1 to 100).map(t => (s, t))).map {
//      case x => Row.fromTuple(x)
//    }
//    val df1 = mjSession.createDataFrame(rdd, StructType(Seq(StructField("x", IntegerType), StructField("z", IntegerType))))
//    val df2 = mjSession.createDataFrame(rdd, StructType(Seq(StructField("x", IntegerType), StructField("y", IntegerType))))
//    val df3 = mjSession.createDataFrame(rdd, StructType(Seq(StructField("y", IntegerType), StructField("z", IntegerType))))
//    df1.createOrReplaceTempView("a")
//    df2.createOrReplaceTempView("b")
//    df3.createOrReplaceTempView("c")
//    Seq(Table("a", df1),Table("b", df2), Table("c", df3))
  }

  val queries: Seq[Benchmarkable] = {
    val queriesFile = currentConfiguration.sparkConf.get(ExperimentConst.QUERIES_FILE).getOrElse("")
    assert(new File(queriesFile).exists(), s"file <${queriesFile}> not exist")
    val sqlText: Seq[(String, String)] = Source.fromFile(queriesFile).getLines().map {
      case line =>
        val i = line.indexOf("\t")
        val name = line.substring(0, i)
        val sqlText = line.substring(i + 1)
        (name, sqlText)
    }.toSeq

    sqlText.map {
      case (name, sql) =>
        Query(mjSession, name = name, sqlText = sql, description = "", executionMode = ForeachResults)
    }
  }
}
