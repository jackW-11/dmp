package com.wjc.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tag06_Location extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(provincename)){
      list:+=("ZP"+provincename,1)
    }
    if (StringUtils.isNotBlank(cityname)){
      list:+=("ZC"+cityname,1)
    }
    list
  }
}
