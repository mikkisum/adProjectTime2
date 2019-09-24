package test

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object JsonTestSecondTime {
  def main(args: Array[String]): Unit = {
    //设置输入输出路径

    if(args.length!=2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath)=args
    //建立连接
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val dataRDD: RDD[String] = sparkSession.sparkContext.textFile(inputPath)
    //dataRDD.collect().toBuffer.foreach(println)
    val mapRdd: RDD[mutable.HashMap[String, String]] = dataRDD.map(line => {
      var map: mutable.HashMap[String, String] = collection.mutable.HashMap[String, String]()
      val jSONObject: JSONObject = JSON.parseObject(line)
      if (jSONObject.getIntValue("status") == 1) {
        val regeocode: JSONObject = jSONObject.getJSONObject("regeocode")
        println("regeocode:"+regeocode.toString())
        if (!regeocode.keySet().isEmpty && regeocode != null) {
          val pois: JSONArray = regeocode.getJSONArray("pois")
          print("pois:")
          pois.toArray().foreach(println)
          if (!pois.isEmpty && pois != null) {
            val arr: Array[AnyRef] = pois.toArray()
            for (ele <- arr) {
              val eleJsonObject: JSONObject = ele.asInstanceOf[JSONObject]
              val businessarea: String = eleJsonObject.getString("businessarea")
              val tp: String = eleJsonObject.getString("type")
              println("businessarea:"+businessarea)
              println("type:"+tp)
              map.+("businessarea" -> businessarea)
              map.+("type" -> tp)

            }
            map
          }
          map
        }
        map
      }
      map
    })
    println("===============================mapRdd===============================")
    mapRdd.collect().toBuffer.foreach(println)
    val businessTag: RDD[(String, Int)] = mapRdd.map(map => {
      val businessarea: String = map.getOrElse("businessarea", "none")
      (businessarea, 1)
    }).reduceByKey(_ + _)
    businessTag.collect().toBuffer.foreach(println)

  }

}
