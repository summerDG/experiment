package org.pasalab.automj.experiment

import org.apache.spark.sql.SparkSession
import org.pasalab.automj.experiment.DefaultDBLP.getClass

object DBLPVariableOrder {
  def main(args: Array[String]): Unit = {
    q11()
    q12()
    q13()
    q14()
    q21()
    q22()
    q31()
    q32()
    q33()
    q34()
    q35()
    q36()
    q36()
    q37()
    q38()
    q39()
  }
  def q11(): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val forder1 = perNa1.as("perNa1").join(perCa1.as("perCa1"), $"perNa1.name1"===$"perCa1.coauthor1").join(pubAu1.as("pubAu1"), $"perCa1.name1"===$"pubAu1.author1")
    val execution = forder1.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q1-order1: $t")
    spark.close()
  }
  def q12(): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val forder2 = perCa1.as("perCa1").join(perNa1.as("perNa1"), $"perNa1.name1"===$"perCa1.coauthor1").join(pubAu1.as("pubAu1"), $"perCa1.name1"===$"pubAu1.author1")
    val execution = forder2.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q1-order2: $t")
    spark.close()
  }
  def q13(): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val forder3 = perCa1.as("perCa1").join(pubAu1.as("pubAu1"), $"perCa1.name1"===$"pubAu1.author1").join(perNa1.as("perNa1"), $"perNa1.name1"===$"perCa1.coauthor1")
    val execution = forder3.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q1-order3: $t")
    spark.close()
  }
  def q14(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val forder4 = pubAu1.as("pubAu1").join(perCa1.as("perCa1"), $"perCa1.name1"===$"pubAu1.author1").join(perNa1.as("perNa1"), $"perNa1.name1"===$"perCa1.coauthor1")
    val execution = forder4.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q1-order4: $t")
    spark.close()
  }

  def q21(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val sorder1 = pubCi1.as("pubCi1").join(pubAu1.as("pubAu1"), $"pubCi1.key1" === $"pubAu1.key1")
      .join(pubAu2.as("pubAu2"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubAu1.author1" === $"pubAu2.author2")
    val execution = sorder1.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q2-order1: $t")
    spark.close()
  }
  def q22(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val sorder2 = pubAu2.as("pubAu2").join(pubAu1.as("pubAu1"), $"pubAu1.author1" === $"pubAu2.author2")
      .join(pubCi1.as("pubCi1"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubCi1.key1" === $"pubAu1.key1")
    val execution = sorder2.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q2-order2: $t")
    spark.close()
  }
  def q31(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val sorder1 = pubCi1.as("pubCi1").join(pubAu1.as("pubAu1"), $"pubCi1.key1" === $"pubAu1.key1")
      .join(pubAu2.as("pubAu2"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubAu1.author1" === $"pubAu2.author2")
    val torder1 = sorder1.join(perCa1.as("perCa1"), $"pubAu1.author1" === $"perCa1.name1").join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1")
    val execution = torder1.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q3-order1: $t")
    spark.close()
  }
  def q32(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val sorder2 = pubAu2.as("pubAu2").join(pubAu1.as("pubAu1"), $"pubAu1.author1" === $"pubAu2.author2")
      .join(pubCi1.as("pubCi1"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubCi1.key1" === $"pubAu1.key1")
    val torder2 = sorder2.join(perCa1.as("perCa1"), $"pubAu1.author1" === $"perCa1.name1").join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1")
    val execution = torder2.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q3-order2: $t")
    spark.close()
  }
  def q33(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val sorder1 = pubCi1.as("pubCi1").join(pubAu1.as("pubAu1"), $"pubCi1.key1" === $"pubAu1.key1")
      .join(pubAu2.as("pubAu2"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubAu1.author1" === $"pubAu2.author2")
    val torder3 = perCa1.as("perCa1").join(sorder1, $"pubAu1.author1" === $"perCa1.name1").join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1")
    val execution = torder3.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q3-order3: $t")
    spark.close()
  }
  def q34(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val sorder2 = pubAu2.as("pubAu2").join(pubAu1.as("pubAu1"), $"pubAu1.author1" === $"pubAu2.author2")
      .join(pubCi1.as("pubCi1"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubCi1.key1" === $"pubAu1.key1")
    val torder4 = perCa1.as("perCa1").join(sorder2, $"pubAu1.author1" === $"perCa1.name1").join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1")
    val execution = torder4.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q3-order4: $t")
    spark.close()
  }
  def q35(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val sorder1 = pubCi1.as("pubCi1").join(pubAu1.as("pubAu1"), $"pubCi1.key1" === $"pubAu1.key1")
      .join(pubAu2.as("pubAu2"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubAu1.author1" === $"pubAu2.author2")
    val torder5 = perCa1.as("perCa1").join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1").join(sorder1, $"pubAu1.author1" === $"perCa1.name1")
    val execution = torder5.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q3-order5: $t")
    spark.close()
  }
  def q36(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val torder6 = pubAu1.as("pubAu1").join(perCa1.as("perCa1"), $"pubAu1.author1"===$"perCa1.name1")
      .join(pubAu2.as("pubAu2"), $"pubAu1.author1"===$"pubAu2.author2")
      .join(pubCi1.as("pubCi1"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubCi1.key1" === $"pubAu1.key1")
      .join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1")
    val execution = torder6.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q3-order6: $t")
    spark.close()
  }
  def q37(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val torder7 = pubAu1.as("pubAu1").join(perCa1.as("perCa1"), $"pubAu1.author1"===$"perCa1.name1")
      .join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1")
      .join(pubAu2.as("pubAu2"), $"pubAu1.author1"===$"pubAu2.author2")
      .join(pubCi1.as("pubCi1"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubCi1.key1" === $"pubAu1.key1")
    val execution = torder7.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q3-order7: $t")
    spark.close()
  }
  def q38(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val torder7 = pubAu1.as("pubAu1").join(pubAu2.as("pubAu2"), $"pubAu1.author1"===$"pubAu2.author2")
      .join(perCa1.as("perCa1"), $"pubAu1.author1"===$"perCa1.name1")
      .join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1")
      .join(pubCi1.as("pubCi1"), $"pubAu2.key2" === $"pubCi1.cite1" && $"pubCi1.key1" === $"pubAu1.key1")
    val execution = torder7.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q3-order8: $t")
    spark.close()
  }
  def q39(): Unit ={
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    import spark.sqlContext.implicits._
    val perCa1 = spark.read.json("wuxiaoqi/dblp/personToCoauthor1").sample(false, 0.1)
    val perNa1 = spark.read.json("wuxiaoqi/dblp/personToName1").sample(false, 0.1)
    val pubAu1 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor1").sample(false, 0.1)
    val pubAu2 = spark.read.json("wuxiaoqi/dblp/publicationToAuthor2").sample(false, 0.1)
    val pubCi1 = spark.read.json("wuxiaoqi/dblp/publicationToCite1").sample(false, 0.1)
    val torder7 = pubAu1.as("pubAu1").join(pubCi1.as("pubCi1"), $"pubCi1.key1" === $"pubAu1.key1")
      .join(perCa1.as("perCa1"), $"pubAu1.author1"===$"perCa1.name1")
      .join(pubAu2.as("pubAu2"), $"pubAu1.author1"===$"pubAu2.author2" && $"pubAu2.key2" === $"pubCi1.cite1")
      .join(perNa1.as("perNa1"), $"perCa1.coauthor1" === $"perNa1.name1")
    val execution = torder7.queryExecution
    execution.executedPlan
    val t = measureTimeMs(execution.toRdd.foreach(_ => Unit))
    println(s"Q3-order9: $t")
    spark.close()
  }


  def measureTimeMs[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }
}
