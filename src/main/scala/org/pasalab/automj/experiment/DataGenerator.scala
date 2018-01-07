package org.pasalab.automj.experiment

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.io.Source

/**
 * The format of the configuration file `tables`,
 * $tableName $fieldName1,$fieldName2...
 */
object DataGenerator {
  def main(args: Array[String]): Unit = {
    assert(args.length == 5, "DataGenerator <tables> <HDFS location> <partitions> <scale> <type>")
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
        (name, schema)
    }

    val conf = new SparkConf().setAppName(getClass.getName)
    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    // 生成json格式的数据
    tables.foreach {
      case (name, schema) =>
        val rdd = sc.parallelize(0 to partitions, partitions).flatMap {
          case id =>
            new RandomIterator(new FrameRowGenerator(schema), scale)
        }
        spark.createDataFrame(rdd, schema).write.json(s"$location${File.separator}$name")
    }
  }
}
