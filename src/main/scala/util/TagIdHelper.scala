package util



import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagIdHelper {

  //设置过滤条件
  val userIdFliterCondition =
    """
      | imei !='' or mac !='' or openudid !='' or androidid !='' or idfa !='' or
      | imeimd5 !='' or macmd5 !='' or openudidmd5 !='' or androididmd5 !='' or idfamd5 !='' or
      | imeisha1 !='' or macsha1 !='' or openudidsha1 !='' or androididsha1 !='' or idfasha1 !=''
    """.stripMargin

  /**
    * 书写获取任意合法id的方法
    */
  def getAnyoneLegalId(row: Row): String = {
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "IM: " + v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => "MC: " + v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => "OD: " + v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) => "AD: " + v.getAs[String]("androidid")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => "ID: " + v.getAs[String]("idfa")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => "IMM: " + v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => "MCM: " + v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => "ODM: " + v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => "ADM: " + v.getAs[String]("androididmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => "IDM: " + v.getAs[String]("idfamd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => "IMS: " + v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) => "MCS: " + v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) => "ODS: " + v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => "ADS: " + v.getAs[String]("androididsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => "IDS: " + v.getAs[String]("idfasha1")

    }
  }

  def getAllLegalId(v: Row): List[String] = {
    val list: List[String] = List[String]()
    if (StringUtils.isNotBlank(v.getAs[String]("imei"))) list :+ "IM: " + v.getAs[String]("imei")
    if (StringUtils.isNotBlank(v.getAs[String]("mac"))) list :+ "MC: " + v.getAs[String]("mac")
    if (StringUtils.isNotBlank(v.getAs[String]("openudid"))) list :+ "OD: " + v.getAs[String]("openudid")
    if (StringUtils.isNotBlank(v.getAs[String]("androidid"))) list :+ "AD: " + v.getAs[String]("androidid")
    if (StringUtils.isNotBlank(v.getAs[String]("idfa"))) list :+ "ID: " + v.getAs[String]("idfa")
    if (StringUtils.isNotBlank(v.getAs[String]("imeimd5"))) list :+ "IMM: " + v.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(v.getAs[String]("macmd5"))) list :+ "MCM: " + v.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(v.getAs[String]("openudidmd5"))) list :+ "ODM: " + v.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(v.getAs[String]("androididmd5"))) list :+ "ADM: " + v.getAs[String]("androididmd5")
    if (StringUtils.isNotBlank(v.getAs[String]("idfamd5"))) list :+ "IDM: " + v.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(v.getAs[String]("imeisha1"))) list :+ "IMS: " + v.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(v.getAs[String]("macsha1"))) list :+ "MCS: " + v.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(v.getAs[String]("openudidsha1"))) list :+ "ODS: " + v.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(v.getAs[String]("androididsha1"))) list :+ "ADS: " + v.getAs[String]("androididsha1")
    if (StringUtils.isNotBlank(v.getAs[String]("idfasha1"))) list :+ "IDS: " + v.getAs[String]("idfasha1")
list
  }
}
