package com.yankee.gmall.realtime.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.yankee.gmall.realtime.common.GmallConfig;
import com.yankee.gmall.realtime.utils.DimUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

@Slf4j
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化Phoenix连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            // 获取数据中为data的
            JSONObject dataJsonObject = jsonObject.getJSONObject("data");

            // 将数据写入Phoenix：upsert into t(id, name, sex) values(..., ..., ...)
            // 获取数据中的key以及value
            Set<String> keys = dataJsonObject.keySet();
            Collection<Object> values = dataJsonObject.values();

            // 获取表名
            String tableName = jsonObject.getString("sink_table");

            // 创建插入数据的SQL
            String upsertSQL = getUpsertSQL(tableName, keys, values);

            // 打印测试
            log.info(upsertSQL);

            // 编译SQL
            preparedStatement = connection.prepareStatement(upsertSQL);

            // 执行
            preparedStatement.executeUpdate();

            // 提交
            connection.commit();

            // 如果是更新操作，则删除redis中的数据，保证数据的一致性
            String type = jsonObject.getString("type");
            if ("update".equals(type)) {
                DimUtil.deleteCached(tableName, dataJsonObject.getString("id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("插入Phoenix数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * 创建upsertSQl
     * @param tableName 表名
     * @param keys 表字段
     * @param values 表字段对应的值
     * @return 更鑫表语句
     */
    private String getUpsertSQL(String tableName, Set<String> keys, Collection<Object> values) {
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName +
                "(" + StringUtils.join(keys, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
