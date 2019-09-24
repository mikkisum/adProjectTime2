package util

import org.apache.commons.logging.{Log, LogFactory}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Redis连接
  */

object JedisConnectionPool {

  //设置最大连接数
  private val config = new JedisPoolConfig()
  config.setMaxTotal(3000)
  config.setMaxIdle(3000)

  private val jedisPool = new JedisPool(config,"hadoop01",6379,10000)
  def getConnection()={
    val log: Log = LogFactory.getLog(this.getClass)
    log.info("make it into \"getConnection\":gettingresource")
    jedisPool.getResource()

  }
  def returnConnection()={

  }
}
