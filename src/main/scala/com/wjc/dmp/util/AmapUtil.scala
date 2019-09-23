package com.wjc.dmp.util
import com.alibaba.fastjson.{JSON, JSONObject}
import scala.collection.mutable.ListBuffer

/**
  *解析经纬度
  * */
object AmapUtil {
  def getBusinessFromAmap(long:Double,lat:Double): String ={
    val location = long+""+lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=8252cb95c8de29c5ff8be39778708a93"
    //调用工具解析
    val infosStr = HttpUtil.get(url)
    //json解析
    val jsonObject = JSON.parseObject(infosStr)
    val status = jsonObject.getIntValue("status")
    val list = ListBuffer[String]()
    if (status==0) return ""
    val geocodes = jsonObject
      .getJSONObject("geocode")
    if (geocodes == null) return ""
    val addressComponent = geocodes
      .getJSONObject("addressComponent")
    if (addressComponent == null) return ""
    val businessAreas = addressComponent
      .getJSONArray("businessAreas")
    if (businessAreas == null) return ""
    for (item <- businessAreas.toArray()){
      if (item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        list.append(name)
      }
    }
    //封装输出
    list.mkString(",")
  }
}
