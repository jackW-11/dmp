package com.wjc.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object Tag05_Keys extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val doc = args(1).asInstanceOf[Broadcast[collection.Map[String,Int]]]
    val keywords = row.getAs[String]("keywords")
    if (StringUtils.isNotBlank(keywords)){
      val infos = keywords.split("\\|")
      for(item<-infos){
        if (item.length >= 3 && item.length<=8 && !doc.value.contains(item)){
          list:+=("K"+item,1)
        }
      }
    }
    list
  }
}
