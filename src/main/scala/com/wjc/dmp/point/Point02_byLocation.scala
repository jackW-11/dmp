package com.wjc.dmp.point

import com.wjc.dmp.util.{PointUtil, Sink2Mysql}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Point02_byLocation {
  def main(args: Array[String]): Unit = {
    if (args.length!=2){
      println("路径不正确")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df = spark.read.parquet(inputPath)
    //saprkcore实现
    df.rdd.map(row=>{
      //获取到相关字段
      val provincename = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")
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
      //封装key和value
      ((provincename,cityname),allist)
    }).reduceByKey((list1,list2)=>list1.zip(list2).map(t=>(t._1+t._2)))
        .collect().foreach(println)
    //saprksql实现
    df.createOrReplaceTempView("df")
    val sql = "select " +
      "provincename,cityname," +
      "sum(case when requestmode=1 then 1 else 0 end) p1," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) p2," +
      "sum(case when requestmode=1 and processnode=3 then 1 else 0 end) p3," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) p4," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) p5," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) p6," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 and iswin=1 and adorderid!=0 then 1 else 0 end) p7," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 and iswin=1 and adorderid!=0 then winprice/1000.0 else 0 end) p8," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 and iswin=1 and adorderid!=0 then adpayment/1000.0 else 0 end) p9 " +
      "from df " +
      "group by provincename,cityname"
    val res = spark.sql(sql)
    res.write.partitionBy("provincename","cityname")
      .mode(SaveMode.Overwrite)
      .json(outputPath)
    Sink2Mysql.tomysql(res,"byLocation")
  }
}
