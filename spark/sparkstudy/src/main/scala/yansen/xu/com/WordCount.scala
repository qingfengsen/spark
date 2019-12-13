package yansen.xu.com


import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val context = new SparkContext(conf)
    val word = context.textFile("D:/sen.txt")
    println("partitions" + word.getNumPartitions)
    val words: RDD[String] = word.flatMap(line => line.split(","))
    val jishuqi: LongAccumulator = context.longAccumulator("jishuqi")


    val tu: RDD[(String, Int)] = words.map(x => {
      if (x == "4") {
        jishuqi.add(1)
      }
      (x, 1)
    })

    println(jishuqi.value)
    println(tu.count())
    println(jishuqi.value)
    jishuqi.reset()
    tu.filter(x => x._2 != 1).foreach(x => println(x._1 + "->" + x._2))
    println(jishuqi.value)
    Thread.sleep(100000)
    context.stop()
  }

}
