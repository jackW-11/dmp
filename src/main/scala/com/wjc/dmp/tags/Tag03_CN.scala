package com.wjc.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object Tag03_CN extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    var adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    if (adplatformproviderid.toString.length>0){
      list:+=("CN"+adplatformproviderid,1)
    }
    list
  }
}
