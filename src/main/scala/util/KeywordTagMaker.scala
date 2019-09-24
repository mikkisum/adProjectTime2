package util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object KeywordTagMaker extends TagRule {
  /**
    * 设置打标签的抽象方法
    * 5)关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
    * 超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
    * 过滤掉停用词库里面的词
    *
    * @param args 可以是任何类型 并且可以是多个
    * @return
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    val row: Row = args(0).asInstanceOf[Row]
    val map: collection.Map[String, Int] = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]].value
    val keywords: Array[String] = row.getAs[String]("keywords").split("\\|").filter(ele=>ele.length<=8&&ele.length>=3&&map.getOrElse(ele,0)!=1)
    val list: List[(String, Int)] = keywords.map(ele=>(ele,1)).toList
    list
  }
}
