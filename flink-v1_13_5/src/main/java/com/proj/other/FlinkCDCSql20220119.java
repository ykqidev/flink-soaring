package com.proj.other;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Objects;

public class FlinkCDCSql20220119 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        env.enableCheckpointing(3000);

//
        String path = "file:///Idea_Projects/workspace/flink-soaring/flink-v1_13_5/cp";
        env.getCheckpointConfig().setCheckpointStorage(path);

        String productsSourceDDL = "CREATE TABLE products (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '192.168.10.100',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'mydb',\n" +
                "    'table-name' = 'products'\n" +
                ")";

        String ordersSourceDDL = "CREATE TABLE orders (\n" +
                "   order_id INT,\n" +
                "   order_date TIMESTAMP(0),\n" +
                "   customer_name STRING,\n" +
                "   price DECIMAL(10, 5),\n" +
                "   product_id INT,\n" +
                "   order_status BOOLEAN,\n" +
                "   PRIMARY KEY (order_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'mysql-cdc',\n" +
                "   'hostname' = '192.168.10.100',\n" +
                "   'port' = '3306',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456',\n" +
                "   'database-name' = 'mydb',\n" +
                "   'table-name' = 'orders'\n" +
                ")";

        String enriched_ordersSinkDDL = "CREATE TABLE enriched_orders (\n" +
                "   order_id INT,\n" +
                "   order_date TIMESTAMP(0),\n" +
                "   customer_name STRING,\n" +
                "   price DECIMAL(10, 5),\n" +
                "   product_id INT,\n" +
                "   order_status BOOLEAN,\n" +
                "   product_name STRING,\n" +
                "   product_description STRING,\n" +
                "   PRIMARY KEY (order_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://192.168.10.100:3306/mydb',\n" +
                "   'table-name' = 'enriched_orders',\n" +
                "   'password' = '123456',\n" +
                "   'username' = 'root'\n" +
                ")";

        String transformSql = "INSERT INTO enriched_orders\n" +
                "SELECT o.*,\n" +
                "       p.name,\n" +
                "       p.description\n" +
                "FROM orders AS o\n" +
                "LEFT JOIN products AS p ON o.product_id = p.id";

        tableEnv.executeSql(productsSourceDDL);
        tableEnv.executeSql(ordersSourceDDL);
        tableEnv.executeSql(enriched_ordersSinkDDL);

        TableResult tableResult = tableEnv.executeSql(transformSql);

        System.out.println("=============================================================================");
        tableResult.print();

        env.execute("sync-flink-cdc");
    }
}
