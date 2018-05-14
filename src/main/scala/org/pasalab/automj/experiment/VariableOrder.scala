package org.pasalab.automj.experiment

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

object VariableOrder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    val tablesFile = args(0)
    val queryPartFile = args(1)
    for (line <- Source.fromFile(tablesFile).getLines()) {
      val parts = line.split("\\s+")
      val name = parts(0)
      val path = parts(1)
      val df = spark.read.json(path)
      df.createTempView(name)
    }
    // triangle
//    val inputString = Seq[String] (
//      "R S R.c=S.c",
//      "R T R.a=T.a",
//      "T S T.b=S.b"
//    )
    // line
//    val inputString = Seq[String] (
//      "R S R.a=S.a",
//      "S T S.b=T.b"
//    )
    // triangle+line
//    val inputString = Seq[String] (
//      "R S R.b=S.b",
//      "S T S.c=T.c",
//      "R T R.a=T.a",
//      "R P R.a=P.a"
//    )

    val remain = mutable.Set[String]()
    val scanned = mutable.Set[String]()
    val edges = mutable.Map[String, mutable.ArrayBuffer[String]]()
    // (table1,table2)->condition
    val conditions = mutable.Map[(String, String), String]()
    for (line <- Source.fromFile(queryPartFile).getLines()) {
//    for(line <- inputString) {
      val parts = line.split("\\s+")
      val t1 = parts(0)
      val t2 = parts(1)
      val c = parts(2)
      conditions += (t1, t2)-> c
      conditions += (t2, t1)-> c
      remain += t1
      remain += t2
      if (edges.contains(t1)) {
        edges(t1).append(t2)
      } else {
        edges += t1 -> mutable.ArrayBuffer[String](t2)
      }
      if (edges.contains(t2)) {
        edges(t2).append(t1)
      } else {
        edges += t2 -> mutable.ArrayBuffer[String](t1)
      }
    }
    val querys = generateAllQuery(remain.toSet, scanned.toSet, edges.toMap, conditions.toMap)
    var avgTime = 0.0
    var minTime = Long.MaxValue
    for(query <- querys) {
      val startTime = System.currentTimeMillis()
      spark.sql(query).queryExecution.toRdd.foreach(_ => Unit)
      val timeout = System.currentTimeMillis() - startTime
      minTime = math.min(minTime, timeout)
      avgTime += timeout
    }
    avgTime = avgTime / querys.length
    println(s"Average Timeout: $avgTime(ms)\nMin Timeout: $minTime(ms)")
  }
  def generateAllQuery(remain: Set[String],
                       scanned: Set[String],
                       edges: Map[String, mutable.ArrayBuffer[String]],
                       conditions: Map[(String, String), String]): Seq[String] = {
    val prefix = "SELECT * FROM "
    val whereClosure = " WHERE "

    conditions.flatMap {
      case ((t1, t2), cond) =>
        tails(remain -- Seq(t1, t2), scanned ++ Seq(t1, t2), edges, conditions, prefix + s"$t1, $t2", whereClosure + cond)
    }.toSeq
  }
  def tails(remain: Set[String],
            scanned: Set[String],
            edges: Map[String, mutable.ArrayBuffer[String]],
            conditions: Map[(String, String), String],
            prefix: String, whereClosre: String): Seq[String] = {
    if (remain.isEmpty) {
      Seq(prefix + whereClosre)
    } else {
      // 1. scanned table
      // 2. nextTable
      val nextTables = scanned.flatMap (x => edges(x)).filter(x => remain.contains(x))
      assert(nextTables.nonEmpty, s"next tables is empty")
      nextTables.flatMap {
        case n =>
          val conds = scanned.flatMap {
            case x =>
              if (conditions.contains(x, n)) Some(conditions(x, n))
              else None
          }
          tails(remain - n, scanned + n, edges, conditions, prefix + s", $n", whereClosre + s" AND ${conds.mkString(" AND ")}")
      }.toSeq
    }
  }
}
