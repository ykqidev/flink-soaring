package com.proj.other;//package com.proj.other;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.shaded.org.joda.time.Instant;
//import org.apache.flink.types.Row;
//
//public class TableAPIOrSQL {
//    public static class User {
//
//        public String name;
//
//        public Integer score;
//
//        public Instant event_time;
//    }
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        DataStreamSource<Row> dataStream = env.fromElements(
//                Row.of("Alice", 12),
//                Row.of("Bob", 10),
//                Row.of("Alice", 100)
//        );
//
//        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
//
//        tableEnv.createTemporaryView("InputTable",inputTable);
//        Table resultTable = tableEnv.sqlQuery("Select name,sum(score) from InputTable GROUP BY name");
//
//        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
//
//        resultStream.print();
//        env.execute();
//
//
//    }
//}
