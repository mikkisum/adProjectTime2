package util

import org.apache.spark.sql.Row

object AdLocationTagMaker extends TagRule {
  /**
    * 取出 广告位id和广告位名称
    * 并按需求处理
    * 广告位类型（标签格式： LC03->1 或者 LC16->1）xx 为数字，小于 10 补 0
    * 1)把广告位类型名称，LN 插屏->1
    * adspacetype: Int,	广告位类型（1：banner 2：插屏 3：全屏）
    * adspacetypename: String,	广告位类型名称（banner、插屏、全屏）
    *
    * @param args 可以是任何类型 并且可以是多个
    * @return
    */
  override def makeTag(args: Any*): List[(String, Int)] = {

    val row: Row = args(0).asInstanceOf[Row]//获取参数列表中的Row类型并向下转型

    var adspacetype: String = String2Type.toString(row.getAs[Int]("adspacetype").toString)
   val adspacetypename: String = String2Type.toString(row.getAs[String]("adspacetypename"))

    if(adspacetype.length<2) adspacetype="0"+adspacetype

    List(("LC"+adspacetype,1),("LN"+adspacetypename,1))


  }
}
