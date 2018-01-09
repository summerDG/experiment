package org.pasalab.automj.experiment

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import scala.io.Source

/**
 * The format of the configuration file `tables`,
 * $tableName $fieldName1,$fieldName2... $path
 */
object DataGenerator {
  def main(args: Array[String]): Unit = {
    assert(args.length == 5, s"DataGenerator(arg len:${args.length}, ${args.mkString(" ")}) <tables> <HDFS location> <partitions> <scale> <type>")
    val fileName = args(0)
    val location = args(1)
    val partitions = args(2).toInt
    val scale = args(3).toInt
    val valueType: DataType = args(4) match {
      case "Int" =>
        IntegerType
      case "Long" =>
        LongType
      case "Double" =>
        DoubleType
      case "Float" =>
        FloatType
      case _ =>
        throw new IllegalArgumentException("Only support Int, Long, Double, Float")
    }

    // 生成表名和对应schema信息
    val tables = Source.fromFile(fileName).getLines().map {
      case line =>
        val pair = line.split("\\s+")
        val name = pair(0)
        val schemaString = pair(1)
        val fields = schemaString.split(",")
          .map(fieldName => StructField(fieldName, valueType, nullable = true))
        val schema = StructType(fields)
        val path: Option[String] = if (pair.length == 3) Some(pair(2)) else None
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
        val rdd: RDD[Row] = path match {
          case Some(p) =>
            sc.textFile(p, partitions).map {
              case line =>
                val pair = line.split("\\s+")
                valueType match {
                  case IntegerType => Row(pair.map (_.toInt):_*)
                  case LongType => Row(pair.map (_.toLong):_*)
                  case DoubleType => Row(pair.map (_.toDouble):_*)
                  case FloatType => Row(pair.map (_.toFloat):_*)
                }
            }
          case _ =>
            sc.parallelize(0 to partitions - 1, partitions).flatMap {
              case id =>
                valueType match {
                  case IntegerType =>
                    new RandomIterator(new FrameRowGenerator(schema, id * scale, Some(scale)), scale)
                  case LongType =>
                    new RandomIterator(new FrameRowGenerator(schema, id * scale, Some(scale)), scale)
                  case _ =>
                    new RandomIterator(new FrameRowGenerator(schema, id), scale)
                }
            }
        }
        spark.createDataFrame(rdd, schema).write.json(s"$location${File.separator}$name")
    }
  }
}
