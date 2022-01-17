package com.proj.other;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.api.Expressions.$;

public class TableAPITest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv  = TableEnvironment.create(settings);
        tEnv.executeSql("CREATE TEMPORARY TABLE kafka_table (\n" +
                "  `message` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'es_topic_zhuying',\n" +
                "  'properties.bootstrap.servers' = '172.16.220.154:9092,172.16.220.153:9092,172.16.220.155:9092',\n" +
                "  'properties.group.id' = 'bi_flink_hive_consumer_group',\n" +
                "  'format' = 'canal-json',\n" +
                "  'scan.startup.mode' = 'group-offsets'\n" +
                ")");

        Table transactions = tEnv.from("kafka_table");
        Table table = transactions.select().limit(10);
        TableResult tableResult = table.execute();
//        tableResult.await();

        tableResult.print();


    }

    public static Table report(Table transactions) {
        return transactions.select(
                $("account_id"),
                $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
                $("amount"))
                .groupBy($("account_id"), $("log_ts"))
                .select(
                        $("account_id"),
                        $("log_ts"),
                        $("amount").sum().as("amount"));
    }
}
