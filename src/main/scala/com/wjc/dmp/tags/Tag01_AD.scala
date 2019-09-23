package com.wjc.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tag01_AD extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    //1)广告位类型（标签格式： LC03->1
    // 或者 LC16->1）xx 为数字，小于 10 补 0，
    // 把广告位类型名称，LN 插屏->1
    val adspacetype = row.getAs[Int]("adspacetype")
    adspacetype match {
      case v if v <= 9 => list:+=("LC 0"+v,1)
      case v if v > 9 => list:+=("LC"+v,1)
      case _ => list:+=("null",0)
    }
    val adspacetypename = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adspacetypename)){
      list:+=("LN "+adspacetypename,1)
    }
    list
  }
}
