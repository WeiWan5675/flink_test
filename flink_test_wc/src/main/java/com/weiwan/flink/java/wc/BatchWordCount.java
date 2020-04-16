package com.weiwan.flink.java.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * @Author: xiaozhennan
 * @Date: 2020/4/16 12:24
 * @Package: com.weiwan.flink.java.wc
 * @ClassName: BatchWordCount
 * @Description:
 **/
public class BatchWordCount {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> ds = env.readTextFile("tmp/a.txt");

        ds.flatMap(new Tokenizer()).groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1);
            }
        }).writeAsText("tmp/b.txt", FileSystem.WriteMode.OVERWRITE);


        env.execute("batch job");
    }


    static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] s = line.split(" ");
            for (String word : s) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    static class WordCountBatch {
        String word;
        Integer count;

        @Override
        public String toString() {
            return word + "=" + count;
        }
    }
}
