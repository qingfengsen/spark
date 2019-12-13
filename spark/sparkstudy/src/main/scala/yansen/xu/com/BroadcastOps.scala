package yansen.xu.com

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastOps {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(s"${BroadcastOps.getClass.getSimpleName}")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val provinces = Map[String, String](
      "62" -> "甘肃",
      "34" -> "安徽",
      "14" -> "山西",
      "44" -> "湖南"
    )

    val cities = Map[String, (String, String)](
      "05" -> ("天水", "62"),
      "20" -> ("淮北", "34"),
      "36" -> ("太原", "14"),
      "25" -> ("韶山", "44")
    )

    //rdd
    val stues = List(
      "1 米鼎 18 1 62 05",
      "2 荆波 19 0 34 20",
      "3 穆书岳 23 0 14 36",
      "4 程旋宇 17 1 44 25"
    )
    val stuRDD: RDD[String] = sc.parallelize(stues)
    val student: RDD[Student] = stuRDD.map(x => {
      val fields: Array[String] = x.split("\\s+")
      Student(fields(0).toInt, fields(1), fields(2).toInt, fields(4), fields(5))
    })
    val provincesBroadcast = sc.broadcast(provinces)
    val citiesBroadcast: Broadcast[Map[String, (String, String)]] = sc.broadcast(cities)
    student.map(x=>{
      val pro: String = provincesBroadcast.value(x.pid)
      val cit: (String) = citiesBroadcast.value(x.cid)._1
      s"${x.id}+${x.name}+${x.age}+${pro}+${cit}"

    }).foreach(println)
    Thread.sleep(100000)
    sc.stop()

  }
  case  class Student(id:Int,name:String,age:Int,pid:String,cid:String)

}
