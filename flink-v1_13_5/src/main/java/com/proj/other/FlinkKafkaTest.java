package com.proj.other;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

public class FlinkKafkaTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.220.154:9092,172.16.220.153:9092,172.16.220.155:9092");
        properties.setProperty("group.id", "bi_flink_hive_consumer_group");

        FlinkKafkaConsumer<ObjectNode> myConsumer = new FlinkKafkaConsumer<>(
                "es_topic_zhuying",
                new JSONKeyValueDeserializationSchema(true),
                properties);

        DataStreamSource<ObjectNode> kafkaSource = env.addSource(myConsumer);
//        kafkaSource.print();

        // 新旧设备状态日志tag
        String[] deviceStatusArr = {
                "ldbi device report deviceStatus V2 : "
                , "ldbi device report deviceStatus : "
                , "ldbi recive laidianStatus : "
        };
        OutputTag<ObjectNode> xjyDeviceOutputTag = new OutputTag<ObjectNode>("xjyDevice_side-output") {
        };
        OutputTag<ObjectNode> deviceOutputTag = new OutputTag<ObjectNode>("device_side-output") {
        };

        SingleOutputStreamOperator<Object> processStream = kafkaSource.process(new ProcessFunction<ObjectNode, Object>() {

            @Override
            public void processElement(ObjectNode jsonNodes, Context context, Collector<Object> collector) throws Exception {
                if (jsonNodes.get("value").has("message")) {
                    String message = jsonNodes.get("value").get("message").toString();
//                    System.out.println(message);
//                    System.exit(0);

                    // 收集设备日志
                    for (String deviceStatus : deviceStatusArr) {
                        if (message.contains(deviceStatus)) {
                            jsonNodes.put("flink_side_out_tag", deviceStatus);
                            if (deviceStatus.equals("ldbi recive laidianStatus : ")) {
                                context.output(xjyDeviceOutputTag, jsonNodes);
                            } else {
                                context.output(deviceOutputTag, jsonNodes);
                            }

                            // 已经找到业务信息直接退出
                        }
                    }
                }
            }
        });

//        processStream.print();

        DataStream<ObjectNode> xjyDeviceSideOutput = processStream.getSideOutput(xjyDeviceOutputTag);
        DataStream<ObjectNode> deviceSideOutput = processStream.getSideOutput(deviceOutputTag);
        xjyDeviceSideOutput.print();
        deviceSideOutput.print();

        env.execute();
    }
}
