package org.pasalab.automj.experiment

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DBLPTransformer {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val f = args(1)
    val s = args(2)
    val tail = args(3)
    val output = args(4)
    val conf = new SparkConf().setAppName(getClass.getName)
    val spark = SparkSession
    .builder().config(conf)
    .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
//    val rdd = spark.read.json(input).rdd
//    val schema = StructType(Array(StructField(f, StringType), StructField(s, StringType)))
//    val df = spark.createDataFrame(rdd, schema)
//    df.show()
//    df.write.json(output)
    formatFile(input, f, s,tail, output, spark, sc)
  }

  private def formatFile(input: String, f: String, s: String,tail:String, output: String, spark: SparkSession, sc: SparkContext) = {
    val rdd = sc.textFile(input).flatMap {
      case l: String =>
        val t = parseToTuple(l, f, s)
        t match {
          case Some(t) =>
            Some(Row.fromTuple(t))
          case None => None
        }
    }
    val schema = StructType(Array(StructField(f+tail, StringType), StructField(s+tail, StringType)))
    val df = spark.createDataFrame(rdd, schema)
    df.show()
    df.write.json(output)
  }

  def parseToTuple(line: String, f:String, s: String): Option[(String,String)] = {
    val sub = line.substring(1, line.length - 1)
    val fId = sub.indexOf("\""+f+"\":")
    val sId = sub.indexOf("\""+s+"\":")
    if (sId < 0 || fId < 0) None
    else {
      var a = ""
      var b = ""
      if (fId < sId) {
        a = sub.substring(fId+f.length+3, sId-1)
        b = sub.substring(sId+s.length+3, sub.length)
      } else {
        b = sub.substring(sId+s.length+3, fId-1)
        a = sub.substring(fId+f.length+3, sub.length)
      }
      Some((a, b))
    }
  }
}
