package com.wjc.dmp.util

object String2Type {
  def toInt(str:String):Int={
    try{
      str.toInt
    }catch {
      case exception: Exception => 0
    }
  }
  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch {
      case exception: Exception => 0.0
    }
  }
}
