package com.wjc.dmp.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object Sink2Redis {
  //连接redis配置
  private val config = new JedisPoolConfig()
  //添加配置
  config.setMaxIdle(10)
  config.setMaxTotal(20)
  //连接池
  private val pool = new JedisPool(config,"hadoop102",6379,10000,"123456")
  def getConnection():Jedis = {
    pool.getResource
  }
}
