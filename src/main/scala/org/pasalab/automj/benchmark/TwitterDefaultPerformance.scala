package org.pasalab.automj.benchmark

import java.io.File

import org.apache.spark.sql.{DataFrame, MjSession}
import org.pasalab.automj.benchmark.ExecutionMode.ForeachResults

import scala.io.Source

/**
 * Created by wuxiaoqi on 18-1-5.
 */
class TwitterDefaultPerformance(mjSession: MjSession) extends Benchmark(mjSession) {
  import mjSession.sqlContext.implicits._

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

    Seq[Query] (
      new Query(mjSession, "square", q1(mjSession), "", None, ExecutionMode.ForeachResults),
      new Query(mjSession, "tri-square", q2(mjSession), "", None, ExecutionMode.ForeachResults),
      new Query(mjSession, "triangle-double", q3(mjSession), "", None, ExecutionMode.ForeachResults)
    )
  }

  def q1(mjSession: MjSession): DataFrame = {
    val ab = mjSession.sql("SELECT * FROM a, b WHERE a.y = b.y")
    val cd = mjSession.sql("SELECT * FROM c, d WHERE c.s = d.s")
    ab.join(cd, $"b.z" === $"c.z" && $"a.x" === $"d.x")
  }

  def q2(mjSession: MjSession): DataFrame = {
    val gh = mjSession.sql("SELECT * FROM g, h WHERE g.q = h.q")
    val abef = mjSession.sql("SELECT * FROM a, b, e, f WHERE a.y = b.y AND b.z = e.z AND e.x = a.x AND e.z = f.z")
    abef.join(gh, $"f.p" === $"g.p" && $"a.y" === $"h.y")
  }

  def q3(mjSession: MjSession): DataFrame = {
    val abef = mjSession.sql("SELECT * FROM a, b, e, f WHERE a.y = b.y AND b.z = e.z AND e.x = a.x AND e.z = f.z")
    val gij = mjSession.sql("SELECT * FROM g, i, j WHERE g.q = i.q AND i.r = j.r AND j.p = g.p")
    abef.join(gij, $"f.p" === $"g.p")
  }
}
