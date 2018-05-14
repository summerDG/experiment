package org.pasalab.automj.experiment

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.pasalab.automj.MjConfigConst

import scala.io.Source

object DefaultDBLP {
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
      if (name == "pubCi1" || name == "perCa1" || name == "perNa1") {

      } else {
        val df = spark.read.json(path)
        df.createTempView(name)
        println(s"Name: $name, Count: ${df.count()}")
      }
    }

    val apa = spark.sql("SELECT * FROM pubAu1, pubAu2 WHERE pubAu1.author1 = pubAu2.author2")
    val pubCite = spark.read.json("wuxiaoqi/dblp/publicationToCite1")
    val q2 = apa.join(pubCite.as("pubCi1"), $"pubAu1.key1" === $"pubCi1.key1" && $"pubAu2.key2" === $"pubCi1.cite1")
    val queryExecution2 = q2.queryExecution
    queryExecution2.executedPlan
    val t2 = measureTimeMs(queryExecution2.toRdd.foreach(_ => Unit))

    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1")
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1")

    val q3 = q2.join(perCa1.as("perCa1"), $"pubAu1.author1" === $"perCa1.name1").join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1")
    val queryExecution3 = q3.queryExecution
    queryExecution3.executedPlan
    val t3 = measureTimeMs(queryExecution3.toRdd.foreach(_ => Unit))

    println(s"Q2: $t2 ms")
    println(s"Q3: $t3 ms")
  }
  def measureTimeMs[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }
}
