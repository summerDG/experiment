package org.pasalab.automj.experiment

import java.io.File

import org.apache.spark.sql.MjSession
import org.apache.spark.sql.catalyst.optimizer.MjOptimizer
import org.apache.spark.sql.catalyst.plans.logical.ShareJoin
import org.apache.spark.{SparkConf, SparkContext}
import org.pasalab.automj.MjConfigConst

import scala.io.Source

object TestMj {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(MjConfigConst.ONE_ROUND_STRATEGY, "org.pasalab.automj.ShareStrategy")
      .set(MjConfigConst.MULTI_ROUND_STRATEGY, "org.pasalab.automj.LeftDepthStrategy")
      .set(MjConfigConst.JOIN_SIZE_ESTIMATOR, "org.pasalab.automj.EstimatorBasedSample")
      .set(MjConfigConst.ONE_ROUND_ONCE, "true")
      .set(MjConfigConst.ONE_ROUND_PARTITIONS, "8")
      .set(MjConfigConst.EXECUTION_MODE, "one-round")
      .set(MjConfigConst.JOIN_DEFAULT_SIZE, "10000000")
      .set(MjConfigConst.SAMPLE_FRACTION, "0.01")
    val sc = SparkContext.getOrCreate(conf)
    val spark = new MjSession(sc)

    assert(spark.sparkContext.getConf.getBoolean(MjConfigConst.ONE_ROUND_ONCE, false), "one-round-once is false")
    assert(spark.sparkContext.getConf.get(MjConfigConst.EXECUTION_MODE) == "one-round",
      s"execution mode ${spark.sparkContext.getConf.get(MjConfigConst.EXECUTION_MODE)}")

    val tablesFile = args(0)
    assert(new File(tablesFile).exists(), s"file <${tablesFile}> not exist")
    val names: Seq[(String, String)] = Source.fromFile(tablesFile).getLines().map {
      case line =>
        val pair = line.split("\\s+")
        assert(pair.length == 2, s"please use correct file format($line), <name> <path>")
        (pair(0), pair(1))
    }.toSeq

    names.foreach {
      case (name, path) =>
        val df = spark.read.json(path)
        df.createOrReplaceTempView(name)
    }
    val df = spark.sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z")
    df.rdd.count()
  }
}
