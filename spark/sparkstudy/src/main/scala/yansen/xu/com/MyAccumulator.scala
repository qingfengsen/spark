package yansen.xu.com

import org.apache.spark.util.AccumulatorV2

object MyAccumulator extends AccumulatorV2{
  override def isZero: Boolean = ???

  override def copy(): AccumulatorV2[Nothing, Nothing] = ???

  override def reset(): Unit = ???

  override def add(v: Nothing): Unit = ???

  override def merge(other: AccumulatorV2[Nothing, Nothing]): Unit = ???

  override def value: Nothing = ???
}
