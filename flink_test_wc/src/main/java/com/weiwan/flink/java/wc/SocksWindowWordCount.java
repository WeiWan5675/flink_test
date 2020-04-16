package com.weiwan.flink.java.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: xiaozhennan
 * @Date: 2020/4/14 13:02
 * @Package: com.weiwan.flink.java.wc
 * @ClassName: SocksWindowWordCount
 * @Description: 通过Socks模拟产生单词数据, flink进行计算
 **/
public class SocksWindowWordCount {
    public static void main(String[] args) throws Exception {

//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("hostname");
//        int port = parameterTool.getInt("port");

        String host = "127.0.0.1";
        int port = 9999;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream(host, port, '\n');

        WindowedStream<WordCountItem, Tuple, TimeWindow> ws = text.flatMap(new FlatMapFunction<String, WordCountItem>() {
            @Override
            public void flatMap(String value, Collector<WordCountItem> collector) throws Exception {
                String[] s = value.split(" ");
                for (String key : s) {
                    collector.collect(new WordCountItem(key, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(1L), Time.seconds(1L));

//        SingleOutputStreamOperator<WordCountItem> reduce = ws.reduce(new ReduceFunction<WordCountItem>() {
//            @Override
//            public WordCountItem reduce(WordCountItem w1, WordCountItem w2) throws Exception {
//                return new WordCountItem(w1.word, w1.count + w2.count);
//            }
//        });
//


        SingleOutputStreamOperator<WordCountItem> count = ws.sum("count");
//        DataStreamSink<WordCountItem> sink = count.print().setParallelism(1);

        DataStreamSink<WordCountItem> wordCountItemDataStreamSink = count.writeAsText("./a.txt").setParallelism(1);


        env.execute();
    }


    public static class WordCountItem {

        public String word;
        public long count;

        public WordCountItem(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public WordCountItem() {
        }

        @Override
        public String toString() {
            return "WordCountItem{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
