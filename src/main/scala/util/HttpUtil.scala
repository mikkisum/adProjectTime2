package util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

object HttpUtil {

  def get(url:String):String={
    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    //发送请求
    val response: CloseableHttpResponse = client.execute(httpGet)
    //处理返回结果 //防止中文乱码
    EntityUtils.toString(response.getEntity,"UTF-8")

  }

}
