package util

object HttpGetTest {
  def main(args: Array[String]): Unit = {
    val url="https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=d974d18aad003b47fa765ae6224020d4"
    val res: String = HttpUtil.get(url)
    println(res)
  }

}
