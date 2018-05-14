package org.pasalab.automj.experiment

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.pasalab.automj.experiment.PPIGenerator.getClass

import scala.io.Source

object TwitterGenerator {
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
        val isLess:Boolean = pair(3) match {
          case "Less" => true
          case "Greater" => false
        }
        (name, schema, path, isLess)
    }

    val conf = new SparkConf().setAppName(getClass.getName)
    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    // 生成json格式的数据
    tables.foreach {
      case (name, schema, path, isLess) =>
        val rdd: RDD[Row] = sc.textFile(path, partitions).flatMap {
          case line =>
            val pair = line.split("\\s+")
            val f = pair(0).toLong
            val s = pair(1).toLong
            if (isLess && f < s) Some(Row.fromTuple((f, s)))
            else if (!isLess && f > s) Some(Row.fromTuple((f, s)))
            else None
        }.distinct()
        spark.createDataFrame(rdd, schema).write.json(s"$location${File.separator}$name")
    }
  }
}
