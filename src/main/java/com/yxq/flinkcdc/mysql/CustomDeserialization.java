package com.yxq.flinkcdc.mysql;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import lombok.Getter;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import com.alibaba.fastjson.JSONObject;
import java.util.List;

/**
 * @author yxq
 * @date 2022-10-18
 */
public class CustomDeserialization implements DebeziumDeserializationSchema<String> {

    /**
     *{
     * "db":""
     * "tablename":"",
     *  befor:json
     *  after:json
     *  op:""
     *  }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db",fields[1]);
        result.putIfAbsent("tableName", fields[2]);

        Struct value = (Struct) sourceRecord.value();
        Struct befor = value.getStruct("before");
        JSONObject beforJson = new JSONObject();

        if (befor != null){
            Schema schema = befor.schema();
            List<Field> fieldsList = schema.fields();

            for (Field field : fieldsList) {
                beforJson.put(field.name(), befor.get(field));
            }
        }
        result.put("before", beforJson);

        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();

        if (after != null){
            Schema schema = after.schema();
            List<Field> fieldsList = schema.fields();

            for (Field field : fieldsList) {
                afterJson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterJson);

        // 操作类型过滤,只处理增删改
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //UPDATE DELETE CREATE
        result.put("op", operation);

        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
