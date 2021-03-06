package org.pasalab.automj.benchmark

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.immutable.Stream
import scala.sys.process._
import org.slf4j.LoggerFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


/**
 * Using ProcessBuilder.lineStream produces a stream, that uses
 * a LinkedBlockingQueue with a default capacity of Integer.MAX_VALUE.
 *
 * This causes OOM if the consumer cannot keep up with the producer.
 *
 * See scala.sys.process.ProcessBuilderImpl.lineStream
 */
object BlockingLineStream {
  // See scala.sys.process.Streamed
  private final class BlockingStreamed[T](
                                           val process:   T => Unit,
                                           val    done: Int => Unit,
                                           val  stream:  () => Stream[T]
                                         )

  // See scala.sys.process.Streamed
  private object BlockingStreamed {
    // scala.process.sys.Streamed uses default of Integer.MAX_VALUE,
    // which causes OOMs if the consumer cannot keep up with producer.
    val maxQueueSize = 65536

    def apply[T](nonzeroException: Boolean): BlockingStreamed[T] = {
      val q = new LinkedBlockingQueue[Either[Int, T]](maxQueueSize)

      def next(): Stream[T] = q.take match {
        case Left(0) => Stream.empty
        case Left(code) =>
          if (nonzeroException) scala.sys.error("Nonzero exit code: " + code) else Stream.empty
        case Right(s) => Stream.cons(s, next())
      }

      new BlockingStreamed((s: T) => q put Right(s), code => q put Left(code), () => next())
    }
  }

  // See scala.sys.process.ProcessImpl.Spawn
  private object Spawn {
    def apply(f: => Unit): Thread = apply(f, daemon = false)
    def apply(f: => Unit, daemon: Boolean): Thread = {
      val thread = new Thread() { override def run() = { f } }
      thread.setDaemon(daemon)
      thread.start()
      thread
    }
  }

  def apply(command: Seq[String]): Stream[String] = {
    val streamed = BlockingStreamed[String](true)
    val process = command.run(BasicIO(false, streamed.process, None))
    Spawn(streamed.done(process.exitValue()))
    streamed.stream()
  }
}

trait DataGenerator extends Serializable {
  def generate(
                sparkContext: SparkContext,
                name: String,
                partitions: Int,
                scaleFactor: String): RDD[String]
}


abstract class Tables(mjSession: MjSession, scaleFactor: String,
                      useDoubleForDecimal: Boolean = false, useStringForDate: Boolean = false)
  extends Serializable {

  def dataGenerator: DataGenerator
  def tables: Seq[Table]

  private val log = LoggerFactory.getLogger(getClass)

  def sparkContext = mjSession.sparkContext

  case class Table(name: String, partitionColumns: Seq[String], fields: StructField*) {
    val schema = StructType(fields)

    def nonPartitioned: Table = {
      Table(name, Nil, fields : _*)
    }

    /**
     *  If convertToSchema is true, the data from generator will be parsed into columns and
     *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
     */
    def df(convertToSchema: Boolean, numPartition: Int) = {
      val generatedData = dataGenerator.generate(sparkContext, name, numPartition, scaleFactor)
      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          mjSession.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            col(f.name).cast(f.dataType).as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        mjSession.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def convertTypes(): Table = {
      val newFields = fields.map { field =>
        val newDataType = field.dataType match {
          case decimal: DecimalType if useDoubleForDecimal => DoubleType
          case date: DateType if useStringForDate => StringType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, newFields:_*)
    }

//    def genData(
//                 location: String,
//                 format: String,
//                 overwrite: Boolean,
//                 clusterByPartitionColumns: Boolean,
//                 filterOutNullPartitionValues: Boolean,
//                 numPartitions: Int): Unit = {
//      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore
//
//      val data = df(format != "text", numPartitions)
//      val tempTableName = s"${name}_text"
//      data.createOrReplaceTempView(tempTableName)
//
//      val writer = if (partitionColumns.nonEmpty) {
//        if (clusterByPartitionColumns) {
//          val columnString = data.schema.fields.map { field =>
//            field.name
//          }.mkString(",")
//          val partitionColumnString = partitionColumns.mkString(",")
//          val predicates = if (filterOutNullPartitionValues) {
//            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
//          } else {
//            ""
//          }
//
//          val query =
//            s"""
//               |SELECT
//               |  $columnString
//               |FROM
//               |  $tempTableName
//               |$predicates
//               |DISTRIBUTE BY
//               |  $partitionColumnString
//            """.stripMargin
//          val grouped = sparkSession.sql(query)
//          println(s"Pre-clustering with partitioning columns with query $query.")
//          log.info(s"Pre-clustering with partitioning columns with query $query.")
//          grouped.write
//        } else {
//          data.write
//        }
//      } else {
//        if (clusterByPartitionColumns) {
//          // treat non-partitioned tables as "one partition" that we want to coalesce
//          data.coalesce(1).write
//        } else {
//          data.write
//        }
//      }
//      writer.format(format).mode(mode)
//      if (partitionColumns.nonEmpty) {
//        writer.partitionBy(partitionColumns : _*)
//      }
//      println(s"Generating table $name in database to $location with save mode $mode.")
//      log.info(s"Generating table $name in database to $location with save mode $mode.")
//      writer.save(location)
//    }

//    def createExternalTable(location: String, format: String, databaseName: String,
//                            overwrite: Boolean, discoverPartitions: Boolean = true): Unit = {
//
//      val qualifiedTableName = databaseName + "." + name
//      val tableExists = sparkSession.tableNames(databaseName).contains(name)
//      if (overwrite) {
//        sparkSession.sql(s"DROP TABLE IF EXISTS $databaseName.$name")
//      }
//      if (!tableExists || overwrite) {
//        println(s"Creating external table $name in database $databaseName using data stored in $location.")
//        log.info(s"Creating external table $name in database $databaseName using data stored in $location.")
//        sparkSession.createExternalTable(qualifiedTableName, location, format)
//      }
//      if (partitionColumns.nonEmpty && discoverPartitions) {
//        println(s"Discovering partitions for table $name.")
//        log.info(s"Discovering partitions for table $name.")
//        sparkSession.sql(s"ALTER TABLE $databaseName.$name RECOVER PARTITIONS")
//      }
//    }

    def createTemporaryTable(location: String, format: String): Unit = {
      println(s"Creating temporary table $name using data stored in $location.")
      log.info(s"Creating temporary table $name using data stored in $location.")
      mjSession.read.format(format).load(location).registerTempTable(name)
    }

    def analyzeTable(databaseName: String, analyzeColumns: Boolean = false): Unit = {
      println(s"Analyzing table $name.")
      log.info(s"Analyzing table $name.")
      mjSession.sql(s"ANALYZE TABLE $databaseName.$name COMPUTE STATISTICS")
      if (analyzeColumns) {
        val allColumns = fields.map(_.name).mkString(", ")
        println(s"Analyzing table $name columns $allColumns.")
        log.info(s"Analyzing table $name columns $allColumns.")
        mjSession.sql(s"ANALYZE TABLE $databaseName.$name COMPUTE STATISTICS FOR COLUMNS $allColumns")
      }
    }
  }

//  def genData(
//               location: String,
//               format: String,
//               overwrite: Boolean,
//               partitionTables: Boolean,
//               clusterByPartitionColumns: Boolean,
//               filterOutNullPartitionValues: Boolean,
//               tableFilter: String = "",
//               numPartitions: Int = 100): Unit = {
//    var tablesToBeGenerated = if (partitionTables) {
//      tables
//    } else {
//      tables.map(_.nonPartitioned)
//    }
//
//    if (!tableFilter.isEmpty) {
//      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
//      if (tablesToBeGenerated.isEmpty) {
//        throw new RuntimeException("Bad table name filter: " + tableFilter)
//      }
//    }
//
//    tablesToBeGenerated.foreach { table =>
//      val tableLocation = s"$location/${table.name}"
//      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
//        filterOutNullPartitionValues, numPartitions)
//    }
//  }

//  def createExternalTables(location: String, format: String, databaseName: String,
//                           overwrite: Boolean, discoverPartitions: Boolean, tableFilter: String = ""): Unit = {
//
//    val filtered = if (tableFilter.isEmpty) {
//      tables
//    } else {
//      tables.filter(_.name == tableFilter)
//    }
//
//    sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
//    filtered.foreach { table =>
//      val tableLocation = s"$location/${table.name}"
//      table.createExternalTable(tableLocation, format, databaseName, overwrite, discoverPartitions)
//    }
//    sparkSession.sql(s"USE $databaseName")
//    println(s"The current database has been set to $databaseName.")
//    log.info(s"The current database has been set to $databaseName.")
//  }

  def createTemporaryTables(location: String, format: String, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createTemporaryTable(tableLocation, format)
    }
  }

  def analyzeTables(databaseName: String, analyzeColumns: Boolean = false, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      table.analyzeTable(databaseName, analyzeColumns)
    }
  }
}
