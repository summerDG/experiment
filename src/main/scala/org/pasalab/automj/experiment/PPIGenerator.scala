package org.pasalab.automj.experiment

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

/**
 * The format of the configuration file `tables`,
 * $tableName $fieldName1,$fieldName2... $path
 */
object PPIGenerator {
  def main(args: Array[String]): Unit = {
    assert(args.length == 3, s"DataGenerator(arg len:${args.length}, ${args.mkString(" ")}) <tables> <HDFS location> <partitions>")
    val fileName = args(0)
    val location = args(1)
    val partitions = args(2).toInt

    // 生成表名和对应schema信息
    val tables = Source.fromFile(fileName).getLines().map {
      case line =>
        val pair = line.split("\\s+")
        val name = pair(0)
        val schemaString = pair(1)
        val fields = schemaString.split(",")
          .map(fieldName => StructField(fieldName, LongType, nullable = true))
        val schema = StructType(fields)
        val path: String = pair(2)
        (name, schema, path)
    }

    val conf = new SparkConf().setAppName(getClass.getName)
    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    // 生成json格式的数据
    tables.foreach {
      case (name, schema, path) =>
        val rdd: RDD[Row] = sc.textFile(path, partitions).flatMap {
          case line =>
            val pair = line.split("\\s+")
            if (pair(0) == "protein1") None
            else {
              val f = pair(0).substring(9).toLong
              val s = pair(1).substring(9).toLong
              val r = if (f < s) Row.fromTuple((f, s)) else Row.fromTuple((s, f))
              Some(r)
            }
        }.distinct()
        spark.createDataFrame(rdd, schema).write.json(s"$location${File.separator}$name")
    }
  }
}
