package org.pasalab.automj.experiment

import java.util

import org.apache.spark.sql.{DataFrame, MjSession, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.{SparkConf, SparkContext}
import org.pasalab.automj.MjConfigConst
import org.pasalab.automj.benchmark.{Benchmark, ExperimentConst, Query}
import org.pasalab.automj.experiment.JoinSizeEstimation.getClass
import org.pasalab.automj.experiment.RunBenchmark.getClass
import org.pasalab.experiment.automj.{Performance, PerformanceFactory, PerformanceSheet}
import org.pasalab.experiment.{GenerateSheet, OutputExcelXlsx, OutputSheetXlsx}

import scala.io.Source
import scala.util.Try

// 求出查询的各部分输出大小
object JoinSizeEvaluation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    assert(args.length == 2, s"JoinSizeEvaluation <Tables File> <Query File>")
    val tablesFile = args(0)
    val queryFile = args(1)
    for (line <- Source.fromFile(tablesFile).getLines()) {
      val pairs = line.split("\\s+")
      assert(pairs.length >= 2, s"File format error: <Table Name> <Path>")
      val name = pairs(0)
      val path = pairs(1)
      val df = spark.read.json(path)
      df.createTempView(name)
      println(s"Name: $name, Count: ${df.count()}")
    }
    for (line <- Source.fromFile(queryFile).getLines()) {
      val pairs = line.split("\t")
      assert(pairs.length >= 4, s"File format error: <Query Name> <SQL> <Tables Count> <default/one-round>")
      spark.sqlContext.setConf(MjConfigConst.EXECUTION_MODE, pairs(3))
      val name = pairs(0)
      val sql = pairs(1)
      val df = spark.sql(sql)
      df.createTempView(name)
      println(s"Query: $name, Count: ${df.count()}, mode: ${pairs(2)}")
    }
  }
}
