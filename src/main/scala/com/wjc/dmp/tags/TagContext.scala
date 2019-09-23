package com.wjc.dmp.tags

import org.apache.spark.sql.SparkSession

object TagContext {
  def main(args: Array[String]): Unit = {
    if (args.length!=3){
      println("路径不正确")
      sys.exit()
    }
    //
    val Array(inputpath,doc,stop) = args
    //
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    val df = spark.read.parquet(inputpath)
    import spark.implicits._
//    df.map(row=>{
//      //id
//      //广告标签
//      //设备、网络、渠道
//      //地区标签
//      //关键字标签
//      //商圈标签
//      ()
//    }).rdd.collect().foreach(println)
  }
}
