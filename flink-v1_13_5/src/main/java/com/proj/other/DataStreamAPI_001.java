package com.proj.other;//package com.proj.other;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
//import org.apache.flink.util.Collector;
//import org.junit.Test;
//
//import java.util.Properties;
//
//public class DataStreamAPI_001 {
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "172.16.220.154:9092,172.16.220.153:9092,172.16.220.155:9092");
//        properties.setProperty("group.id", "bi_flink_hive_consumer_group");
//
//        FlinkKafkaConsumer<ObjectNode> myConsumer = new FlinkKafkaConsumer<>(
//                "es_topic_zhuying",
//                new JSONKeyValueDeserializationSchema(true),
//                properties);
//
//        DataStreamSource<ObjectNode> kafkaSource = env.addSource(myConsumer);
//        SingleOutputStreamOperator<ObjectNode> filter = kafkaSource.filter((FilterFunction<ObjectNode>) value -> {
//            if (value.get("value").has("message")) {
//                String string = value.get("value").get("message").toString();
//                return string.contains("ldbi recive laidianStatus");
//            }
//            return false;
//        })
//                .flatMap(new FlatMapFunction<ObjectNode, ObjectNode>() {
//            @Override
//            public void flatMap(ObjectNode value, Collector<ObjectNode> out) throws Exception {
////                System.out.println(value);
//                out.collect((ObjectNode) value.get("value").get("agent"));
//            }
//        });
//
//        filter.print();
//        env.execute();
//    }
//
//}
