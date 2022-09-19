package com.cdc.mysql.v2_0_2;

import com.cdc.mysql.debezium.MyJsonDebeziumDeserializationSchema;
import com.google.gson.Gson;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

//import com.ververica.cdc.connectors.mysql.MySqlSource;

public class MySqlSourceExampleTest {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.10.100")
                .port(3306)
                .databaseList("mydb") // set captured database
//                .tableList("mydb.orders")
                .username("root")
                .password("123456")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .deserializer(new MyJsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
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
}
