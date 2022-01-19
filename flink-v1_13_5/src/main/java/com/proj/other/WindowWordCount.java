package com.proj.other;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.*;

public class WindowWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<Tuple2<String, Integer>> dataStream = env
//                .socketTextStream("localhost", 9999)
//                .flatMap(new Splitter())
//                .keyBy(value -> value.f0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .sum(1);
////
//        dataStream.print();

        DataStreamSource<String> dataStream = env
                .socketTextStream("localhost", 9999);

        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word: sentence.split(" ")) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).keyBy(value -> value.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0,value1.f1+1);
            }
        }).print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
