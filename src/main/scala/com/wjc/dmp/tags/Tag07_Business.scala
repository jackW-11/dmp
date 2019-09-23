package com.wjc.dmp.tags

import ch.hsr.geohash.GeoHash
import com.wjc.dmp.util.{AmapUtil, Sink2Redis}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tag07_Business extends Tag {

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val long = row.getAs[Double]("long")
    val lat = row.getAs[Double]("lat")
    //得到经纬度消息
    //对经纬度用geo维护
    val business = getBusiness(long,lat)
    //拿到商圈信息后进行切割
    if (StringUtils.isNotBlank(business)){
      val strs = business.split(",")
      strs.foreach(str=>{
        list:+=(str,1)
      })
    }
    list
  }

  def getBusiness(long: Double, lat: Double):String = {
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    //从redis取数据
    var business = redis_queryBusiness(geohash)
    if (business==null){
      //去高德请求
      business = AmapUtil.getBusinessFromAmap(long,lat)
      //存入redis
      if (business!=null && business.length>0){
        redis_saveBusiness(geohash,business)
      }
    }
    business
  }
  def redis_queryBusiness(geohash: String):String = {
    val jedis = Sink2Redis.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  def redis_saveBusiness(geohash: String, business: String) = {
    val jedis = Sink2Redis.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
