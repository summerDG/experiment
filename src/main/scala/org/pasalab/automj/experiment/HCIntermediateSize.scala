package org.pasalab.automj.experiment

import org.pasalab.automj.GenerateShares

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * HCIntermediateSize <tableFile> <nodesNum>
  * The format of tableFile: <TableName> <Variables...> <Size>
  */
object HCIntermediateSize {
  val tables: ArrayBuffer[ArrayBuffer[Int]] = new ArrayBuffer[ArrayBuffer[Int]]()
  val sizes: ArrayBuffer[Long] = new ArrayBuffer[Long]()
  def main(args: Array[String]): Unit = {
    val tableSchemaFile = args(0)
    val originNum = args(1).toInt
    val AttributeId: mutable.Map[String, Int] = new mutable.HashMap[String, Int] ()
    for (line <- Source.fromFile(tableSchemaFile).getLines()) {
      val pairs = line.split("\\s+")
      val name = pairs(0)
      tables += new ArrayBuffer[Int]()
      val tableId = tables.length - 1
      for(i <- 1 to pairs.length - 2) {
        if (AttributeId.contains(pairs(i))) {
          tables(tableId) += AttributeId(pairs(i))
        } else {
          val aId = AttributeId.size
          AttributeId.put(pairs(i), aId)
          tables(tableId) += aId
        }
      }
      sizes += pairs(pairs.length - 1).toLong
    }
    val base = math.log(originNum)

    def muis(sizes: Seq[Long]) : Seq[Double] = {
      assert(sizes.forall(_ > 0), "sizes error")
      sizes.map(s => math.log(s) / base)
    }

    val originShares = GenerateShares.generateLp(muis(sizes).toArray, tables.map(_.map(_ + 1).toArray).toArray, originNum,
      AttributeId.size)
    val roundDownShares = originShares.map (x => if (x > 1) x.asInstanceOf[Int] else 1)
    val x = roundDownShares.fold(1)(_ * _)
    val roundDownNum = if (x>originNum) originNum else x
    val randomShares = GenerateShares.generateLp(muis(sizes).toArray, tables.map(_.map(_ + 1).toArray).toArray, originNum * 10,
      AttributeId.size).map (x => if (x > 1) x.asInstanceOf[Int] else 1)
    val (shares, num) = roundShares(originShares, originNum)
    val intermediateSize = tables.zip(sizes).map {
      case (attrs, size) =>
        (0 to shares.length - 1).filter(x => !attrs.contains(x)).map(shares).fold(1)(_ * _) * size
    }.sum

    val nodeNum = if (num > originNum) originNum else num
    println(s"shares: ${shares.mkString("[", ",", "]")}, nodes num: $nodeNum($num cells), workload:${nw(shares, nodeNum)}, intermediate size: $intermediateSize")
    println(s"roundDownShares: ${roundDownShares.mkString("[", ",", "]")}, nodes num: $roundDownNum($x cells), workload:${nw(roundDownShares, roundDownNum)}")
    println(s"randomShares: ${randomShares.mkString("[", ",", "]")}, nodes num: $originNum(${originNum*10}cells), workload:${nw(randomShares, originNum)}")
    println(s"optimalShares: ${originShares.mkString("[", ",", "]")}, nodes num: $originNum, workload:${nw(originShares, originNum)}")
  }
  def muis(sizes: Seq[Long], base: Long) : Seq[Double] = {
    assert(sizes.forall(_ > 0), "sizes error")
    sizes.map(s => math.log(s) / base)
  }
  def roundShares(shares: Array[Double], origin: Int): (Array[Int], Int) = {
    // enumarate all cases
    val caseNums = math.pow(2, shares.length).asInstanceOf[Int]
    val cases = new Array[Array[Int]](caseNums).map(_ => new Array[Int](shares.length))
    for (col <- 0 until shares.length) {
      val part = caseNums / math.pow(2, col + 1).asInstanceOf[Int]
      for (row <- 0 until caseNums) {
        // 如果得出来的share正好是一个整数，那么就不用找邻居了
        if (shares(col) - shares(col).asInstanceOf[Int] == 0) {
          cases(row)(col) = shares(col).asInstanceOf[Int]
        } else {
          if ((row / part) % 2 == 0) {
            cases(row)(col) = shares(col).asInstanceOf[Int]
          } else {
            cases(row)(col) = shares(col).asInstanceOf[Int] + 1
          }
        }
      }
    }
    var minError = origin
    var novelShares: Array[Int] = null
    var novel = 1
    for (row <- 1 until caseNums) {
      val tmp = cases(row).fold(1)(_ * _)
      val tmpError = ((tmp - origin).abs)
      if (tmp > 0) {
        if (novelShares == null) {
          novel = tmp
          novelShares = cases(row)
          minError = tmpError
        }
        else if (tmpError < minError
          || (tmpError == minError && nw(cases(row), tmp) < nw(novelShares, origin))) {
          novel = tmp
          novelShares = cases(row)
          minError = tmpError
        }
      }
    }

    (novelShares, novel)
  }
  def nw(shares: Array[Int], n: Int): Double = {
    val newNum = shares.fold(1)(_ * _)
    val nodeNum = if (newNum > n) n else newNum
    var load: Long = 0L
    for (i <- 0 until tables.length) {
      load += (0 to shares.length - 1).filter(x => !tables(i).contains(x))
        .map(shares).fold(1)(_ * _) * sizes(i)
    }
    load / nodeNum.asInstanceOf[Double]
  }
  def nw(shares: Array[Double], n: Int): Double = {
    var load: Double = 0.0
    for (i <- 0 until tables.length) {
      load += (0 to shares.length - 1).filter(x => !tables(i).contains(x))
        .map(shares).fold(1.0)(_ * _) * sizes(i)
    }
    load / n.asInstanceOf[Double]
  }

}
