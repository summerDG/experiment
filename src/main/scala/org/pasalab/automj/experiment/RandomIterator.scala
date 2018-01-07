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

  override def next(): T = r.next
}
trait RowGenerator[T] {
  def next: T
}
class FrameRowGenerator(schema: StructType) extends RowGenerator[Row] {
  val rowGen = generateFields()
  def generateFields() = {
    schema.fields.map {
      case f =>
        f.dataType match {
          case IntegerType =>
            IntRandomField(new Random())
          case LongType =>
            LongRandomField(new Random())
          case DoubleType =>
            DoubleRandomField(new Random())
          case FloatType =>
            FloatRandomField(new Random())
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

case class IntRandomField(r: Random) extends RandomField[Int] {
  override def next: Int = {
    r.nextInt()
  }
}
case class LongRandomField(r: Random) extends RandomField[Long] {
  override def next: Long = {
    r.nextLong()
  }
}
case class DoubleRandomField(r: Random) extends RandomField[Double] {
  override def next: Double = {
    r.nextDouble()
  }
}
case class FloatRandomField(r: Random) extends RandomField[Float] {
  override def next: Float = {
    r.nextFloat()
  }
}
