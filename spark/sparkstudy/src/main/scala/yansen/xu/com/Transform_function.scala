package yansen.xu.com

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transform_function {
  def main(args: Array[String]): Unit = {
     val conf: SparkConf = new SparkConf()
      .setAppName(s"${Transform_function.getClass}")
      .setMaster("local[*]")
     val context: SparkContext = new SparkContext(conf)
//    rbk(context)
//    gbk(context)
    joinexm(context)
    context.stop()
  }
  private def rbk(context: SparkContext): Unit ={
    val list = List("hello ni hao",
      "hello ni hao",
      "hello ni hao",
      "hello ni hao")
    val rdd: RDD[String] = context.parallelize(list)
    val value: RDD[(String, Int)] = rdd.flatMap(_.split("\\s+"))
      .map((_,1)).reduceByKey(_+_)
    value.foreach{case(a,b)=>{
      println(s"${a} => ${b}")
    }}

  }
  private def gbk(context: SparkContext): Unit ={
    val list = List("hello ni hao",
      "hello ni hao",
      "hello ni hao",
      "hello ni hao")
    val rdd: RDD[String] = context.parallelize(list)
    val value: RDD[(String, Iterable[Int])] = rdd.flatMap(_.split("\\s+"))
      .map((_,1)).groupByKey()
    value.foreach(a=>println(s"${a._1} => ${a._2.size}"))
  }
  def joinexm(context: SparkContext): Unit ={
    val stu = List(
      "1 周攀 19",
      "2 宋时杰 23",
      "3 王政禄 18",
      "4 俞剑波 22"
    )
    val score = List(
      "1 chinese 65 4",
      "2 math 95 1",
      "3 english 59.5 3",
      "4 chinese 72 2",
      "5 art 70 6"
    )
    val studentRDD: RDD[String] = context.parallelize(stu)
    val scoreRDD: RDD[String] = context.parallelize(score)

    val studentMap: RDD[(String, String)] = studentRDD.map(line => {
      val fields = line.split("\\s+")
      val id = fields(0)
      val name = fields(1)
      val age = fields(2)
      (id, s"${name}\t${age}")
    })
    val scoreMap: RDD[(String, String)] = scoreRDD.map(line => {
      val fields = line.split("\\s+")
      val id = fields(0)
      val cource = fields(1)
      val score = fields(2)
      (id, s"${cource}\t${score}")
    })
    val result: RDD[(String, (String, String))] = studentMap.join(scoreMap)
    result.foreach{case (a,b) =>
    println(s"${a} => ${b._1}\t${b._2}")}

  }


}
