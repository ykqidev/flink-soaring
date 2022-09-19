package com.cdc.mysql.debezium;

import com.google.gson.Gson;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

public class MyJsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    public MyJsonDebeziumDeserializationSchema() {
    }


    /**
     * 将cdc数据反序列化
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {
        Gson gson = new Gson();
        HashMap<String, Object> hashMap = new HashMap<>();

//        System.out.println("sourceRecord==> " + sourceRecord);
        String topic = sourceRecord.topic();
        System.out.println("topic==> " + topic);
        String[] split = topic.split("[.]");
        String database = split[1];
        String table = split[2];
        hashMap.put("database", database);
        hashMap.put("table", table);
        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        System.out.println(operation);
        System.out.println("=======================================================");
        //获取数据本身
        Struct struct = (Struct) sourceRecord.value();
        Struct after = struct.getStruct("after");

        if (after != null) {
            Schema schema = after.schema();
            HashMap<String, Object> hm = new HashMap<>();
            for (Field field : schema.fields()) {
                hm.put(field.name(), after.get(field.name()));
            }
            hashMap.put("data", hm);
        }

        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        hashMap.put("type", type);

        collector.collect(gson.toJson(hashMap));

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
