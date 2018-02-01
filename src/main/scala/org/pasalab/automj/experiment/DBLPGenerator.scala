package org.pasalab.automj.experiment

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.pasalab.automj.experiment.DataGenerator.getClass

import scala.collection.mutable.ArrayBuffer

object DBLPGenerator {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val conf = new SparkConf().setAppName(getClass.getName)
    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()
    val publicationToAuthor = spark.read.json(input)
    val df = publicationToAuthor.groupBy("key").agg(Map(
      "author"->"collect_set"
    ))
    val coauthors = df.flatMap {
      case r: Row =>
        val coauthors = r.getSeq[String](1)
        val buf = ArrayBuffer[AuthorPair]()
        val n = coauthors.length - 1
        for(i <- 0 to n; j <- 0 to n if i != j) {
          buf += (AuthorPair(coauthors(i), coauthors(j)))
        }
        buf
    }.distinct()
    coauthors.write.json(output)
  }
  case class AuthorPair(name: String, coauthor: String)
}
