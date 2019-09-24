package com.wjc.dmp.gra

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object GraTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("g1")
      .getOrCreate()
    //创建点
    val point = spark.sparkContext.makeRDD(Seq(
      (1L, ("小黑", 24)),
      (3L, ("小白", 24)),
      (4L, ("小赤", 24)),
      (6L, ("小橙", 24)),
      (133L, ("小黄", 24)),
      (135L, ("小绿", 24)),
      (158L, ("小青", 24)),
      (13L, ("小蓝", 24)),
      (16L, ("小紫", 24))
    ))
    //创建边
    val edge = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(3L, 133L, 0),
      Edge(4L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(6L, 135L, 0),
      Edge(16L, 135L, 0),
      Edge(13L, 158L, 0)
    ))
    edge
    //创建图形
    val gra = Graph(point,edge)
    //选顶点
    val vertices = gra.connectedComponents().vertices
    vertices.foreach(println)
    //取值打印输出
    vertices.join(point).map {
      case (id, (d,(name, age))) => {(d,List(name, age))}
    }.reduceByKey(_++_)
      .foreach(println)
  }
}
