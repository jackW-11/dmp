package com.wjc.dmp.tags

import org.apache.spark.sql.Row

object Tag04_Dev extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args.asInstanceOf[Row]
    val ispname = row.getAs[String]("ispname")
    val networkmannername = row.getAs[String]("networkmannername")
    val client = row.getAs[Int]("client")
    ispname match {
      case "移动" => list:+=("D00030001",1)
      case "联通" => list:+=("D00030002",1)
      case "电信" => list:+=("D00030003",1)
      case _ => list:+=("D00030004",1)
    }
    networkmannername match {
      case "WIFI" => list:+=("D00020001",1)
      case "4G" => list:+=("D00020002",1)
      case "3G" => list:+=("D00020003",1)
      case "2G" => list:+=("D00020004",1)
      case _ => list:+=("D00020005",1)
    }
    client match {
      case 1 => list:+=("Android D00010001",1)
      case 2 => list:+=("IOS D00010002",1)
      case  3 => list:+=("WinPhone D00010003",1)
      case _ => list:+=("其 他 D00010004",1)
    }
    list
  }
}
