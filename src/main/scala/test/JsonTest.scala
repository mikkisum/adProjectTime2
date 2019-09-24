package test


import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * json数据归纳格式（考虑获取到数据的成功因素 status=1成功 starts=0 失败）：
  * 1、按照pois，分类businessarea，并统计每个businessarea的总数。
  * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  */

object JsonTest {
  def main(args: Array[String]): Unit = {
    //读取文件
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    //val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = new SparkContext(conf)
    val strings: RDD[String] = sparkContext.textFile("E:\\gitPro\\adProjectTime2\\doc\\json.txt")
    // strings.collect().toBuffer.foreach(println)

    //处理解析
    val rdd: RDD[String] = strings.flatMap(t => {
      val jsonObject: JSONObject = JSON.parseObject(t)
      var result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
      val status: Int = jsonObject.getIntValue("status")
      if (status == 0) {}
      else {
        val regeocode: JSONObject = jsonObject.getJSONObject("regeocode")
        if (regeocode == null || regeocode.keySet().isEmpty()) {}
        else {
          val poisArray: JSONArray = regeocode.getJSONArray("pois")
          if (poisArray.isEmpty || poisArray == null) {}
          else {

            for (ele <- poisArray.toArray()) {
              val eleJsonObject: JSONObject = ele.asInstanceOf[JSONObject]
              val businessarea: String = eleJsonObject.getString("businessarea")
              val tp: String = eleJsonObject.getString("type")
              val info = businessarea + "," + tp
              result.append(info)

            }
            result

          }
          result
        }
        result
      }
      result
    })
//    val businessTup = rdd.map(str => {
//      val group: Array[String] = str.split(",")
//      val businessArea: String = group(0)
//      (businessArea, 1)
//    })

    val businessTup: RDD[(String, Int)] = rdd.filter(_.split(",")(0) != "[]").map(str => {
      val group: Array[String] = str.split(",")
      val businessArea: String = group(0)
      (businessArea, 1)
    })
    businessTup
    val typeRdd: RDD[String] = rdd.flatMap(str => {
      val group: Array[String] = str.split(",")
      val types: String = group(1)
      types.split(";")
    })
    val typeTup: RDD[(String, Int)] = typeRdd.map(tp => (tp, 1))
    println("=================BUSINESS_AREA========================")
    val businessRes: RDD[(String, Int)] = businessTup.reduceByKey(_ + _)
    businessRes.collect().toBuffer.foreach(println)
    println("=====================TYPE======================")
    val typeRes: RDD[(String, Int)] = typeTup.reduceByKey(_ + _)
    typeRes.collect().toBuffer.foreach(println)
    sparkContext.stop()
  }


}
