package com.wjc.dmp.test

object T1 {
  def main(args: Array[String]): Unit = {
   var list1 = List((1,2,3),(7,4,5))
    var list2 = (1,2,3)
    list1 :+= list2
    println(list1)
  }
}
