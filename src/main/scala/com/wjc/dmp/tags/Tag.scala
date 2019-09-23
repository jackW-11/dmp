package com.wjc.dmp.tags

trait Tag {
  def makeTags(args:Any*):List[(String,Int)]
}
