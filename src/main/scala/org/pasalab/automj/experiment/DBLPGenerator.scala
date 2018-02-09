package org.pasalab.automj.experiment

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.pasalab.automj.experiment.DataGenerator.getClass

import scala.collection.mutable.ArrayBuffer

object DBLPGenerator {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val n = args(2)
    val conf = new SparkConf().setAppName(getClass.getName)
    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()
    import spark.implicits._
    val publicationToAuthor = spark.read.json(input)
    val df = publicationToAuthor.groupBy(s"key$n").agg(Map(
      s"author$n"->"collect_set"
    ))

    n match {
      case "1" =>
        val coauthors = df.flatMap(getAllAuthor1).distinct()
        coauthors.write.json(output)
      case "2" =>
        val coauthors = df.flatMap(getAllAuthor2).distinct()
        coauthors.write.json(output)
      case "3" =>
        val coauthors = df.flatMap(getAllAuthor3).distinct()
        coauthors.write.json(output)
    }
  }

  def getAllAuthor1(r: Row): Seq[AuthorPair1] = {
    val coauthors = r.getSeq[String](1)
    val buf = ArrayBuffer[AuthorPair1]()
    val n = coauthors.length - 1
    for(i <- 0 to n; j <- 0 to n if i != j) {
      buf += (AuthorPair1(coauthors(i), coauthors(j)))
    }
    buf
  }
  def getAllAuthor2(r: Row): Seq[AuthorPair2] = {
    val coauthors = r.getSeq[String](1)
    val buf = ArrayBuffer[AuthorPair2]()
    val n = coauthors.length - 1
    for(i <- 0 to n; j <- 0 to n if i != j) {
      buf += (AuthorPair2(coauthors(i), coauthors(j)))
    }
    buf
  }
  def getAllAuthor3(r: Row): Seq[AuthorPair3] = {
    val coauthors = r.getSeq[String](1)
    val buf = ArrayBuffer[AuthorPair3]()
    val n = coauthors.length - 1
    for(i <- 0 to n; j <- 0 to n if i != j) {
      buf += (AuthorPair3(coauthors(i), coauthors(j)))
    }
    buf
  }
  case class AuthorPair1(name1: String, coauthor1: String)
  case class AuthorPair2(name2: String, coauthor2: String)
  case class AuthorPair3(name3: String, coauthor3: String)
}
