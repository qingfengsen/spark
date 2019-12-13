package yansen.xu.com

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
/*chinese,ls,91
english,ww,56
chinese,zs,90
chinese,zl,76
english,zq,88
chinese,wb,95
chinese,sj,74
english,ts,87
english,ys,67
english,mz,77
chinese,yj,98
english,gk,96*/

object TopN_optimize {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TopN_optimize").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val test: RDD[String] = sc.textFile("file:///D:/study/spark/spark_共享变量，排序09/代" +
      "码/spark-study-1811/data/topn.txt")
    val testMap: RDD[(String, String)] = test.map(x => {
      val xname: String = x.substring(0, x.indexOf(","))
      val yname: String = x.substring(x.indexOf(",") + 1)
      (xname, yname)
    })
 /*   combineByKey为了将键值对按照建进行分组，K代表键，V代表值，C代表V通过函数转换之后的类型，可以与V的类型不同
      createCombiner是将值装入一个集合，一个键只会执行一次这个方法
      mergeValue将同一个分区中的同一个键的剩余的值和上面的集合进行合并
      mergeCombiners将不同分区中的同一个键的值进行合并
      def combineByKey[C](
          createCombiner: V => C,
          mergeValue: (C, V) => C,
          mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
      combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
    }*/
    testMap.combineByKey(createCombiner,mergeValue,mergeCombiners)
      .foreach(println)
   /* (english,TreeSet(ww,56, ys,67, mz,77))
    (chinese,TreeSet(sj,74, zl,76, zs,90))*/

    sc.stop()
  }

  def createCombiner(nameScore:String):mutable.TreeSet[String]={
    val res: mutable.TreeSet[String] = new mutable.TreeSet[String]()(new Ordering[String] {
      override def compare(x: String, y: String): Int = {
        val xScore: String = x.substring(x.indexOf(",") + 1)
        val yScore: String = y.substring(y.indexOf(",") + 1)
        xScore.compareTo(yScore)
      }
    })
    res.add(nameScore)
    res
  }

  def mergeValue(nameScores:mutable.TreeSet[String],nameScore:String):mutable.TreeSet[String]={
    nameScores.add(nameScore)
    if(nameScores.size>3){
      nameScores.dropRight(1)
    }else{
      nameScores
    }
  }

  def mergeCombiners( res1:mutable.TreeSet[String],res2:mutable.TreeSet[String]):mutable.TreeSet[String]={
  res1++=(res2)
    if(res1.size>3){
      res1.dropRight(res1.size-3)
    }else{
      res1
    }
  }


}
