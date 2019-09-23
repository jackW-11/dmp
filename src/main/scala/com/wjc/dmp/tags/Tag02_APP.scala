package com.wjc.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object Tag02_APP extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val doc = args(1).asInstanceOf[Broadcast[collection.Map[String,String]]]
    val appid = row.getAs[String]("appid")
    var appname = row.getAs[String]("appname")
    if (StringUtils.isBlank(appname)){
      appname = doc.value.getOrElse(appid,"unknow")
    }
    list:+=("APP"+appname,1)
    list
  }
}
