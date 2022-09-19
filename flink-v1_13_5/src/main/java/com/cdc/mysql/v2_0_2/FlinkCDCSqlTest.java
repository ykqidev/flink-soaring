package com.cdc.mysql.v2_0_2;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * flink-connector-mysql-cdc_2.0.2
 */
public class FlinkCDCSqlTest {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        env.enableCheckpointing(3000);

//        String path = "file:///Idea_Projects/workspace/flink-soaring/flink-v1_13_5/cp";
        String path = "hdfs://linux100:8020/ck/cp";
        env.getCheckpointConfig().setCheckpointStorage(path);

        //两个检查点之间间隔时间，默认是0,单位毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        //Checkpoint过程中出现错误，是否让整体任务都失败，默认值为0，表示不容忍任何Checkpoint失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);

        //Checkpoint是进行失败恢复，当一个 Flink 应用程序失败终止、人为取消等时，它的 Checkpoint 就会被清除
        //可以配置不同策略进行操作
        // DELETE_ON_CANCELLATION: 当作业取消时，Checkpoint 状态信息会被删除，因此取消任务后，不能从 Checkpoint 位置进行恢复任务
        // RETAIN_ON_CANCELLATION(多): 当作业手动取消时，将会保留作业的 Checkpoint 状态信息,要手动清除该作业的 Checkpoint 状态信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //Flink 默认提供 Extractly-Once 保证 State 的一致性，还提供了 Extractly-Once，At-Least-Once 两种模式，
        // 设置checkpoint的模式为EXACTLY_ONCE，也是默认的，
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置checkpoint的超时时间, 如果规定时间没完成则放弃，默认是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        //设置同一时刻有多少个checkpoint可以同时执行，默认为1就行，以避免占用太多正常数据处理资源
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //设置了重启策略, 作业在失败后能自动恢复,失败后最多重启3次，每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        // 当有较新的 Savepoint 时，作业也会从 Checkpoint 处恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

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

        tableEnv.executeSql(transformSql).print();
        System.out.println("=============================================================================");
        env.execute("sync-flink-cdc");
    }
}
