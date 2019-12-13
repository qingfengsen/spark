package yansen.xu.com

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object TopN {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val test: RDD[String] = sc.textFile("file:///D:\\study\\spark\\spark_共享变量，排序09\\代码\\spark-study-1811\\data\\topn.txt")
    val gMap: RDD[(String, Iterable[String])] = test.map(x => {
      val xname: String = x.substring(0, x.indexOf(","))
      val yname: String = x.substring(x.indexOf(",") + 1)
      (xname, yname)
    }).groupByKey()
    gMap.foreach(println)
    val orderRes: RDD[(String, mutable.TreeSet[String])] = gMap.map { case (course, nameCores) => {
      var res = new mutable.TreeSet[String]()(new Ordering[String] {
        override def compare(x: String, y: String): Int = {
          val xScore: String = x.split(",")(1)
          val yScore: String = y.split(",")(1)
          xScore.compareTo(yScore)
        }
      })
      for (nameCore <- nameCores) {
        res.add(nameCore)
        if (res.size > 3) {
          res = res.dropRight(1)//dropRight函数会返回一个新的Rdd，所以需要res来接收，否则不会变化
        }
      }
      (course, res)
    }
    }
    orderRes.foreach(println)
    sc.stop()
    
    
  }

}
