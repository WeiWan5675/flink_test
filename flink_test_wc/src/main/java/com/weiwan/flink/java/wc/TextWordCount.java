package com.weiwan.flink.java.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: xiaozhennan
 * @Date: 2020/4/14 13:40
 * @Package: com.weiwan.flink.java.wc
 * @ClassName: TextWordCount
 * @Description:
 **/
public class TextWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile("./a.txt");

        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {


            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = line.split(",");
                for (String key : split) {
//                   collector.collect();
                }
            }
        });


    }

    public static class WordCountItemBatch {

        public String key;
        public Long count;

        public WordCountItemBatch(String key, Long count) {
            this.key = key;
            this.count = count;
        }

        public WordCountItemBatch() {
        }

        @Override
        public String toString() {
            return key + count;
        }
    }
}
