package com.weiwan.flink.scala.utils

object CommonUtils {


  def getSubStr(str: String) = {
    str + "getSubStr";
  }


  def main(args: Array[String]): Unit = {
    var a = "mainfangfan";
    print(getSubStr(a))
    var v = getInt(1,2)
    print(v)
  }



  def getInt(a: Integer,b: Integer) = {
    a + b;
  }

}
