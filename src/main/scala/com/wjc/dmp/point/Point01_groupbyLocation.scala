package com.wjc.dmp.point

import com.wjc.dmp.util.Sink2Mysql
import org.apache.spark.sql.{SaveMode, SparkSession}

object Point01_groupbyLocation {
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
    import org.apache.spark.sql.functions._
    val res = df.groupBy("provincename", "cityname")
      .agg(count("*") as "counts")
    res.write.mode(SaveMode.Overwrite)
      .partitionBy("provincename","cityname")
      .parquet(outputPath)
    Sink2Mysql.tomysql(res,"groupbylocation")
  }
}
