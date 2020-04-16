package com.weiwan.flink.scala.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * @Author: xiaozhennan
 * @Date: 2020/4/16 13:09
 * @Package: com.weiwan.flink.scala.wc
 * @ClassName: BatchWordCountScala
 * @Description:
 **/
object BatchWordCountScala {

  def main(args: Array[String]): Unit = {

    val environment = ExecutionEnvironment.getExecutionEnvironment
    val text = environment.readTextFile("tmp/a.txt")
    text.flatMap(_.toLowerCase().split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .writeAsText("tmp/b.txt", WriteMode.OVERWRITE).setParallelism(1)

    environment.execute("scala batch job")
  }
}
