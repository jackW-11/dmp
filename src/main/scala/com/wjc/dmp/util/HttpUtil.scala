package com.wjc.dmp.util

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object HttpUtil {
  def get(url:String):String={
    val client = HttpClients.createDefault()
    val get = new HttpGet(url)
    val response = client.execute(get)
    //防止乱码
    EntityUtils.toString(response.getEntity,"UTF-8")
  }

}
