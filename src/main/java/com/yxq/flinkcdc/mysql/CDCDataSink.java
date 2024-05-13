package com.yxq.flinkcdc.mysql;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONObject;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Date;
import java.util.Map;

/**
 * @author Mi
 * @date 2022-10-18
 */
public class CDCDataSink extends RichSinkFunction {

    // 初始化方法
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    @Override
    public void invoke(Object value, Context context) throws Exception {
        String v = value.toString();
        System.out.println(">>>>>>>" + v);
        JSONObject obj = JSONObject.parseObject(v);
        String op = obj.getString("op");
        String tableName = obj.getString("tableName");
        String db = obj.getString("db");
        String sql = "";
        if (op.equals("CREATE") ) {
            JSONObject afterObj = obj.getJSONObject("after");
            String columns = "";
            String vals = "";
            String updates = "";

            for (Map.Entry<String, Object> entry : afterObj.entrySet()) {
                String key = entry.getKey();
                Object valObj = entry.getValue();
                if ("create_time".equals(key)) {
                    valObj = DateUtil.formatDateTime(new Date());;
                }
                columns += "`" + key + "`,";
                if (valObj instanceof String) {
                    vals += "'" + valObj + "',";
                    updates += "`" + key + "`='" + valObj + "',";
                } else {
                    vals += valObj + ",";
                    updates += "`" + key + "`=" + valObj + ",";
                }
            }
            if (columns.endsWith(",")) {
                columns = columns.substring(0, columns.length() - 1);
            }
            if (vals.endsWith(",")) {
                vals = vals.substring(0, vals.length() - 1);
            }
            if (updates.endsWith(",")) {
                updates = updates.substring(0, updates.length() - 1);
            }
            sql = " INSERT INTO `" + db + "`.`" + tableName + "` (" + columns + ") VALUES " + "(" + vals + ")" +
                    " ON DUPLICATE KEY UPDATE " + updates;
        }else if (op.equals("UPDATE")) {
            JSONObject beforeObj = obj.getJSONObject("before");
            JSONObject afterObj = obj.getJSONObject("after");
            StringBuilder setClause = new StringBuilder();

            for (Map.Entry<String, Object> entry : afterObj.entrySet()) {
                String key = entry.getKey();
                Object valObj = entry.getValue();

                // 检查字段是否在beforeObj中不同，或者beforeObj中不存在
                if (!beforeObj.containsKey(key) || !beforeObj.get(key).equals(valObj)) {
                    // 格式化字段值
                    String formattedValue = formatValue(valObj);

                    // 添加到更新语句中
                    setClause.append("`").append(key).append("`=").append(formattedValue).append(", ");
                }
            }

            // 移除最后一个多余的逗号和空格
            if (setClause.length() > 0) {
                setClause.setLength(setClause.length() - 2);
            }

            // 构建完整的更新SQL语句
            String id = afterObj.getString("id");
            sql = "UPDATE `" + db + "`.`" + tableName + "` SET " + setClause.toString() + " WHERE id='" + id + "'";
        }
        else if (op.equals("DELETE")) {
            JSONObject beforeObj = obj.getJSONObject("before");
            String id = beforeObj.getString("id");
            sql = " DELETE FROM `" + db + "`.`" + tableName + "` where id='" + id + "'";
        } else {
            System.out.println(">>>>>>> 当前只处理 增CREATE、改UPDATE、删DELETE");
        }
        if (StringUtils.isNotEmpty(sql)) {
            System.out.println(">>>>>>>" + sql);
            //  保存数据库
            int upSert = MySqlDBUtils.executeSql(sql);
            System.out.println(">>>>>>>>>>>>插入|更新  ->>>" + (upSert > 0 ? "成功" : "失败") + "<<<");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    private String formatValue(Object valObj) {
        if (valObj instanceof String) {
            return "'" + valObj.toString() + "'";
        } else if (valObj instanceof Date) {
            return   DateUtil.formatDateTime((Date) valObj);
        } else {
            return valObj.toString();
        }
    }
}
