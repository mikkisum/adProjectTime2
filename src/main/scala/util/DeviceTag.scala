package util

import org.apache.spark.sql.Row

object DeviceTag extends TagRule {
  /**
    * 设置打标签的抽象方法
    *
    * @param args 可以是任何类型 并且可以是多个
    * @return
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    //设备操作系统
    val client: Int = row.getAs[Int]("client")
    client match {
      case 1 => list:+=("D0001001",1)
      case 2 => list:+=("D0001002",1)
      case 3 => list:+=("D0001003",1)
      case _ => list:+=("D0001004",1)
    }
    //设备联网方式
    val networkmannername: String = row.getAs[String]("networkmannername")
    networkmannername match {
      case "WIFI" => list:+=("D00020001 ",1)
      case "4G" => list:+=("D00020002",1)
      case "3G" => list:+=("D00020003",1)
      case "2G" => list:+=("D00020004",1)
      case _ => list:+=("D00020005",1)

    }
    //设备运营方式
    val ispname: String = row.getAs[String]("ispname")
    ispname match {
      case "移动" => list:+=("D0001001",1)
      case "联通" => list:+=("D0001002",1)
      case "电信" => list:+=("D0001003",1)
      case _ => list:+=("D0001004",1)
    }
    list
  }
}
