package com.wjc.dmp.point

import com.wjc.dmp.util.{PointUtil, Sink2Mysql}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

object Point07_byMedia {
  def main(args: Array[String]): Unit = {
    if (args.length!=3){
      println("路径不正确")
      sys.exit()
    }
    val Array(inputPath,outputPath,docmap) = args
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df = spark.read.parquet(inputPath)
    //saprkcore实现
    //处理字典文件并广播
    val docRdd = spark.sparkContext.textFile(docmap)
    val doc = docRdd.map(_.split("\\s", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(0))).collectAsMap()
    val docBro = spark.sparkContext.broadcast(doc)
    import spark.implicits._
    val res = df.rdd.map(row=>{
      //获取到相关字段
      var appname = row.getAs[String]("appname")
      val appid = row.getAs[String]("appid")
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      //调用函数
      val list1 = PointUtil.requestNum(requestmode,processnode)
      val list2 = PointUtil.showNum(requestmode,iseffective)
      val list3 = PointUtil.rtbNum(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      val allist = list1++list2++list3
      //处理key
      if (StringUtils.isBlank(appname)){
        appname = docBro.value.getOrElse(appid,"unknow")
      }
      //封装key和value
      (appname,allist)
    }).reduceByKey((list1,list2)=>list1.zip(list2).map(t=>(t._1+t._2)))
      .map(row=>{
        (row._1,
          row._2.mkString(",").split(",")(0),
          row._2.mkString(",").split(",")(1),
          row._2.mkString(",").split(",")(2),
          row._2.mkString(",").split(",")(3),
          row._2.mkString(",").split(",")(4),
          row._2.mkString(",").split(",")(5),
          row._2.mkString(",").split(",")(6),
          row._2.mkString(",").split(",")(7),
          row._2.mkString(",").split(",")(8)
        )
      })
      .toDF("appname","p1","p2","p3","p4","p5","p6","p7","p8","p9")
    res.write.mode(SaveMode.Overwrite)
      .partitionBy("appname")
      .json(outputPath)
    Sink2Mysql.tomysql(res,"byMedia")
  }
}
