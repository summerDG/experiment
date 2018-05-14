package org.pasalab.automj.experiment

import org.apache.spark.sql.SparkSession

import scala.io.Source

// 求出查询的各部分输出大小
object TwitterQueryCount {
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
    val ab = spark.sql("SELECT * FROM a, b WHERE a.y = b.y")
    val cd = spark.sql("SELECT * FROM c, d WHERE c.s = d.s")
    val q1 = ab.join(cd, $"b.z" === $"c.z" && $"a.x" === $"d.x")
    val gh = spark.sql("SELECT * FROM g, h WHERE g.q = h.q")
    val abef = spark.sql("SELECT * FROM a, b, e, f WHERE a.y = b.y AND b.z = e.z AND e.x = a.x AND e.z = f.z")
    val q2 = abef.join(gh, $"f.p" === $"g.p" && $"a.y" === $"h.y")
    val gij = spark.sql("SELECT * FROM g, i, j WHERE g.q = i.q AND i.r = j.r AND j.p = g.p")
    val q3 = abef.join(gij, $"f.p" === $"g.p")

    println(s"Q1: ${q1.count()}")
    println(s"Q2: ${q2.count()}")
    println(s"Q3: ${q3.count()}")
  }

}
