package com.cdc.mysql.v2_2_0;

import com.cdc.mysql.debezium.MyJsonDebeziumDeserializationSchema;
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySqlSourceExampleTest {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.10.100")
                .port(3306)
                .databaseList("mydb") // set captured database
//                .tableList("mydb.orders,mydb.products")
                .username("root")
                .password("123456")
//                .serverId("5401-5404")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .deserializer(new MyJsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .includeSchemaChanges(true)
                .build();
//        SourceFunction<String> mySqlSource = (SourceFunction<String>) mySqlSource1;
//        MySqlSource<String> mySqlSource = mySqlSource;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println(env);
        // enable checkpoint
        env.enableCheckpointing(3000);


        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
