package com.weiwan.flink.scala.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


object SocketWordCountScala {


  def main(args: Array[String]): Unit = {

    //创建运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取端口参数
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception =>
        System.err.println("no port set,use default port 9999")
        //制定默认端口
        9999
    }
    //定义服务器IP
    val host: String = "127.0.0.1";
    //数据解析
    val text = env.socketTextStream(host, port,'\n')


    import org.apache.flink.api.scala._

    val counts = text.flatMap(line => line.toLowerCase.split(","))
      .map(w => WordWithCountScala(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(2))
      .sum("count")
      .print().setParallelism(1)


    env.execute("socks streaming job")



  }


  case class WordWithCountScala(word: String, count: Long)

}
