package org.pasalab.automj.benchmark

import java.io.File

import org.apache.spark.sql.DataFrame
import org.pasalab.automj.benchmark.ExecutionMode.ForeachResults

import scala.io.Source

/**
 * Created by wuxiaoqi on 18-1-5.
 */
class AutoMjPerformance extends Benchmark {

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
        (name, sqlContext.read.json(path))
    }
    dfs.map {
      case (name, df) =>
        // 这里必须先注册, 否则后续的sql会找不到表名
        df.createOrReplaceTempView(name)
        Table(name, df)
    }
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
        Query(name = name, sqlText = sql, description = "", executionMode = ForeachResults)
    }
  }
}
