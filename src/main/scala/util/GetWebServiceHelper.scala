//package util
//
//import java.io.OutputStream
//import java.net.{HttpURLConnection, URL}
//
//import com.alibaba.fastjson.JSONObject
//import org.apache.commons.logging.{Log, LogFactory}
//
//
//object GetWebServiceHelper {
//  val log: Log = LogFactory.getLog(this.getClass)
//  //https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=d974d18aad003b47fa765ae6224020d4&radius=1000&extensions=all
//  def send(lat:String,long:String):JSONObject={
//    val str="https://restapi.amap.com/v3/geocode/regeo?output=json&location="+long+","+lat+"&key=d974d18aad003b47fa765ae6224020d4"
//    val url = new URL(str)
//    val httpURLConnection: HttpURLConnection = url.openConnection().asInstanceOf[HttpURLConnection]
//    httpURLConnection.setRequestMethod("POST")
//    httpURLConnection.setRequestProperty("content-type","charset=utf-8")
//    httpURLConnection.setDoInput(true)
//    httpURLConnection.setDoOutput(true)
//    //发送请求
//    val os: OutputStream = httpURLConnection.getOutputStream()
//
//
//  }
//}
