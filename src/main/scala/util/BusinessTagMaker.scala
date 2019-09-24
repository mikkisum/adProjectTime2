package util

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer



object BusinessTagMaker extends TagRule {
  /**
    * 设置打标签的抽象方法
    *
    * @param args 可以是任何类型 并且可以是多个
    * @return
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    val log: Log = LogFactory.getLog(this.getClass)
    log.info("make it into \"makeTag\"")
    val row: Row = args(0).asInstanceOf[Row]
    val lat: Double = String2Type.toDouble(row.getAs[String]("lat"))
    val long: Double = String2Type.toDouble(row.getAs[String]("long"))
    val business: String = getBusiness(long,lat)
    //====================================================================================================
    var listBuffer = new ListBuffer[(String,Int)]()
    if(StringUtils.isNotBlank(business)){
      val array: Array[(String, Int)] = business.split(",").map(business=>(business,1))
      array.toList.toBuffer.foreach(println)
      array.map(ele=>listBuffer.append(ele))
    }
    listBuffer.toList
  }

  /**
    * 获取商圈信息 利用geohash 从高德服务获取
    *
    */
  def getBusiness(long:Double,lat:Double):String={
    val log: Log = LogFactory.getLog(this.getClass)
    log.info("make it into \"getBusiness\"")
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    //首先查看redis里面是否已经存在信息
    var businessArea=""
    if(getBusinessFromRedis(geoHash)!=null){
      businessArea=getBusinessFromRedis(geoHash)
    }
    else{
      businessArea=AmapUtil.getBusinessAreaFromAmap(long,lat)
      if(!businessArea.isEmpty||(!businessArea.equals(""))){
        println("*************************businessArea is not Empty ,it is \""+businessArea+"\"**************************")
        saveIntoRedis(geoHash,businessArea)}

    log.info("\"geoHash:\""+geoHash)
    log.info("\"businessArea_FromRedis:\""+businessArea)

    }

  businessArea
  }

  /**
    * 从数据中查询
    */
  def getBusinessFromRedis(geoHash: String):String={
    val log: Log = LogFactory.getLog(this.getClass)
    log.info("make it into \"getBusinessFromRedis\"")
    val jedis: Jedis = JedisConnectionPool.getConnection()
    val businessArea: String = jedis.get(geoHash)
    log.info("get business from redis:businessArea========"+businessArea)
   // log.info("businessArea is null?="+businessArea.isEmpty)
    businessArea

  }

  //将经纬度转化成geohash码

  /**
    * 如果查不到，存入到Redis
    */
  def saveIntoRedis(geoHash:String,businessArea:String)={
    val log: Log = LogFactory.getLog(this.getClass)
    log.info("make it into \"saveIntoRedis\"")
    val jedis: Jedis = JedisConnectionPool.getConnection()
    log.info("make it into \"getConnection\":connection successfully got")
    jedis.set(geoHash,businessArea)
    
  }
}
