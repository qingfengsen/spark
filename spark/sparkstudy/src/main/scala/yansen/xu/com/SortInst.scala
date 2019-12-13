package yansen.xu.com

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object SortInst {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(s"${SortInst.getClass.getSimpleName}")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val list = List(
      "hello you",
      "i hate you",
      "i love you",
      "i love you",
      "fuck you"
    )
    val words: RDD[String] = sc.parallelize(list).flatMap(x=>x.split("\\s+"))
    val wordsMap: RDD[(String, Int)] = words.map((_,1)).reduceByKey((x, y)=>x+y)
//    val wordSord: RDD[(Int, String)] = wordsMap.map(x => (x._2, x._1)).sortByKey(true, 2)
//    wordSord.collect().take(3).foreach(println)
//    wordsMap.sortBy(t=>(t._2,t._1),true,1).foreach(println)
//    重写ordering方法
    wordsMap.sortBy(t=>t,true,1)(new Ordering[(String,Int)] {
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        var ret = x._2.compareTo(y._2)
        if(ret == 0){
          ret = x._1.compareTo(y._1)
        }
        ret
      }
    },ClassTag.Object.asInstanceOf[ClassTag[(String,Int)]] ).foreach(println)
    sc.stop()


  }

}
