package org.pasalab.automj.experiment

import org.apache.spark.sql.SparkSession

import scala.io.Source

object PPIQueryCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    assert(args.length == 1, s"JoinSizeEvaluation <Tables File>")
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
    val q1 = spark.sql("SELECT * FROM R, S, T WHERE R.b = S.b AND S.c = T.c")
    val q2 = spark.sql("SELECT * FROM R, S, P WHERE R.b = S.b AND S.c = P.c AND P.a = R.a")
    val rsp = spark.sql("SELECT * FROM R, S, P WHERE R.b = S.b AND S.c = P.c AND P.a = R.a")
    val tq = spark.sql("SELECT * FROM Q, T WHERE Q.d = T.d")
    val q3 = rsp.join(tq, $"R.a" === $"Q.a" && $"S.c" === $"T.c")

    println(s"Q1: ${q1.count()}")
    println(s"Q2: ${q2.count()}")
    println(s"Q3: ${q3.count()}")
  }

}
