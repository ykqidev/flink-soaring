package com.proj.other;

import com.google.gson.Gson;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

//import com.ververica.cdc.connectors.mysql.MySqlSource;

public class MySqlSourceExample {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.10.100")
                .port(3306)
                .databaseList("mydb") // set captured database
                .tableList("mydb.orders")
                .username("root")
                .password("123456")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
//        SourceFunction<String> mySqlSource = (SourceFunction<String>) mySqlSource1;
//        MySqlSource<String> mySqlSource = mySqlSource;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println(env);
        // enable checkpoint
        env.enableCheckpointing(3000);


        env
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .addSource(mySqlSource)
//                 set 4 parallel source tasks
//                .setParallelism(4)
                .print(); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }

    // 将cdc数据反序列化
    public static class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema {
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {

            Gson jsstr = new Gson();
            HashMap<String, Object> hs = new HashMap<>();

            System.out.println("sourceRecord==> " +sourceRecord);
            String topic = sourceRecord.topic();
            System.out.println("topic==> " +topic);
            String[] split = topic.split("[.]");
            String database = split[1];
            String table = split[2];
            hs.put("database",database);
            hs.put("table",table);
            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            System.out.println(operation);
            System.out.println("=======================================================");
            //获取数据本身
            Struct struct = (Struct)sourceRecord.value();
            Struct after = struct.getStruct("after");

            if (after != null) {
                Schema schema = after.schema();
                HashMap<String, Object> afhs = new HashMap<>();
                for (Field field : schema.fields()) {
                    afhs.put(field.name(), after.get(field.name()));
                }
                hs.put("data",afhs);
            }

            String type = operation.toString().toLowerCase();
            if ("create".equals(type)) {
                type = "insert";
            }
            hs.put("type",type);

            collector.collect(jsstr.toJson(hs));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

}
