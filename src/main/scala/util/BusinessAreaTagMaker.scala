//package util
//
//import org.apache.spark.sql.Row
//
//object BusinessAreaTagMaker extends TagRule {
//  /**
//    * 设置打标签的抽象方法
//    *
//    * @param args 可以是任何类型 并且可以是多个
//    * @return
//    */
//  override def makeTag(args: Any*): List[(String, Int)] = {
//    val row: Row = args(0).asInstanceOf[Row]
//    //取到经纬度
//    val long: String = row.getAs[String]("long")//经度
//    val lat: String = row.getAs[String]("lat")
//    //从web服务获取商圈信息
//    //格式化标签
//  }
//}
