package util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.ListBuffer

object AmapUtil {

  def getBusinessAreaFromAmap(long: Double, lat: Double): String = {
    val log: Log = LogFactory.getLog(this.getClass)
    log.info("make it into \"Amap\"")
    //https://restapi.amap.com/v3/geocode/regeo? output=json&
    // output=xml&location=116.310003,39.991957&key=d974d18aad003b47fa765ae6224020d4
    val location = long + "," + lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?location=" + location + "&key=d974d18aad003b47fa765ae6224020d4"
    //webservice
    val jsonstr: String = HttpUtil.get(url)
    println("======================jsonstr=" + jsonstr)
    //解析json
    val jSONObject: JSONObject = JSON.parseObject(jsonstr)
    val status: Int = jSONObject.getIntValue("status")
    println("======================status=" + status)
    if (status == 0) return ""
    else {
      val regeocode: JSONObject = jSONObject.getJSONObject("regeocode")
      if (regeocode == null || regeocode.keySet().isEmpty) return ""
      else {
        println("================regeocode=" + regeocode.toString)
        val addressComponent: JSONObject = regeocode.getJSONObject("addressComponent")
        if (addressComponent == null || addressComponent.keySet().isEmpty) return ""
        else {
          println("================addressComponent=" + addressComponent.toString)
          val businessArea: JSONArray = addressComponent.getJSONArray("businessAreas")
          if (businessArea == null || businessArea.isEmpty) return ""
          else {
            println("==============businessArea_printAnyway=" + businessArea.toString + "is it empty:" + businessArea.isEmpty)
            println("================businessArea=" + businessArea.toString)
            val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
            for (item <- businessArea.toArray()) {
              if (item.isInstanceOf[JSONObject]) {
                val json: JSONObject = item.asInstanceOf[JSONObject]
                val name: String = json.getString("name")
                println("=================name=" + name)
                result.append(name)
              }
            }
            result.mkString(",")

          }

        }
      }

    }
  }
}
