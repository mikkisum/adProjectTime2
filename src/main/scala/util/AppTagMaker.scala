package util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object AppTagMaker extends TagRule {
  /**
    * 设置打标签的抽象方法
    *
    * @param args 可以是任何类型 并且可以是多个
    * @return
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    val row: Row = args(0).asInstanceOf[Row]
    val map: collection.Map[String, String] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]].value
    //获取字段
    val appid: String = row.getAs[String]("appid")
    var appname: String = row.getAs[String]("appname")
    if(StringUtils.isBlank(appname)) {appname=map.getOrElse(appid,appid)}
    else {appname=appname}
    List((appname,1))
  }
}
