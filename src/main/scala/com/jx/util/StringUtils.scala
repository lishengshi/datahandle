package com.jx.util

object StringUtils {

  def isLong(string: String):Boolean={
    try{
      val int: Integer = Integer.valueOf(string)
      true
    }catch{
      case e:Exception=>false
    }
  }

  def isNum(string: String):Boolean={
    try{
      Integer.valueOf(string)
      true
    }catch{
      case e:Exception=>false
    }
  }



}
