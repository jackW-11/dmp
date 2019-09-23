package com.wjc.dmp.util

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode}

object Sink2Mysql {
  def tomysql(df:DataFrame,table:String):Unit={
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    df.write.mode(SaveMode.Overwrite)
      .jdbc(load.getString("jdbc.url"),table,prop)
    println("数据库写入完成")
  }
}
