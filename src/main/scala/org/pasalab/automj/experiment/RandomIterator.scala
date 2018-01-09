package org.pasalab.automj.experiment

import java.util.Random

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Created by wuxiaoqi on 18-1-7.
 */
class RandomIterator[T](r: RowGenerator[T], scale: Int) extends Iterator[T] {
  var count = 0
  override def hasNext: Boolean = count < scale

  override def next(): T = {
    assert(hasNext, "all values are generated")
    count += 1
    r.next
  }
}
trait RowGenerator[T] {
  def next: T
}

/**
 * 生成一行数据的生成器
 * @param schema
 * @param bound 只有数据类型是Int的时候有用
 */
class FrameRowGenerator(schema: StructType, start: Int,  bound: Option[Int] = None) extends RowGenerator[Row] {
  val rowGen = generateFields()
  def generateFields() = {
    schema.fields.map {
      case f =>
        f.dataType match {
          case IntegerType =>
            IntRandomField(new Random(), start, bound)
          case LongType =>
            LongRandomField(new Random(), start, bound)
          case DoubleType =>
            DoubleRandomField(new Random(), start)
          case FloatType =>
            FloatRandomField(new Random(), start)
          case _ =>
            throw new IllegalArgumentException("The generator only supports Int, Long, Double, Float")
        }
    }
  }
  override def next: Row = {
    Row(rowGen.map(_.next):_*)
  }
}
trait RandomField[T] {
  def next: T
}

case class IntRandomField(r: Random, start: Int, bound: Option[Int] = None) extends RandomField[Int] {
  override def next: Int = {
    bound match {
      case Some(b) =>
        r.nextInt(b) + start
      case _ =>
        r.nextInt() + start
    }
  }
}
case class LongRandomField(r: Random, start: Int, bound: Option[Int]) extends RandomField[Long] {
  override def next: Long = {
    bound match {
      case Some(b) =>
        r.nextInt(b).toLong + start
      case _ =>
        r.nextLong() + start
    }
  }
}
case class DoubleRandomField(r: Random, start: Int) extends RandomField[Double] {
  override def next: Double = {
    r.nextDouble() + start
  }
}
case class FloatRandomField(r: Random, start: Int) extends RandomField[Float] {
  override def next: Float = {
    r.nextFloat() + start
  }
}
