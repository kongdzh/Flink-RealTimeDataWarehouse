package com.yankee.gmall.realtime.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.yankee.gmall.realtime.bean.TableProcess;
import com.yankee.gmall.realtime.common.GmallConfig;
import com.yankee.gmall.realtime.utils.MySQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class DBSplitProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    // 定义属性
    private OutputTag<JSONObject> outputTag;

    // 定义配置信息的Map
    private HashMap<String, TableProcess> tableProcessHashMap;

    // 定义Set用于记录当前Phoenix中是否已经存在该表
    private HashSet<String> existTables;

    // 定义Phoenix的连接
    private Connection connection = null;

    public DBSplitProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化配置信息的Map
        tableProcessHashMap = new HashMap<>();

        // 初始化Phoenix已经存在的表的Set集合
        existTables = new HashSet<>();

        // 初始化Phoenix的连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // 读取配置信息
        refreshMeta();

        // 开启定时调度任务，周期性读取配置信息方法
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 10000L, 5000L);
    }

    /**
     * 周期性调度方法
     * 1.读取MySQL配置信息
     * 2.将查询结果封装为Map，以便于后续每条数据获取
     * 3.检查Phoenix中是否存在该表，如果不存在，则在Phoenix中创建该表
     */
    private void refreshMeta() {
        log.info("开始读取MySQL配置信息！");

        // 1.读取MySQL中的配置信息
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class,
                true);

        // 2.将查询结果封装为Map，以便于后续每条数据获取
        for (TableProcess tableProcess : tableProcesses) {
            // 获取源表
            String sourceTable = tableProcess.getSourceTable();

            // 获取操作类型
            String operateType = tableProcess.getOperateType();

            String key = sourceTable + ":" + operateType;
            tableProcessHashMap.put(key, tableProcess);

            // 3.检查Phoenix中是否存在该表，如果不存在，则在Phoenix中创建该表
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                // 校验Phoenix中是否已经存在该表
                boolean notExist = existTables.add(tableProcess.getSinkTable());
                if (notExist) {
                    checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(),
                            tableProcess.getSinkExtend());
                }
            }
        }

        // 校验
        if (tableProcessHashMap == null || tableProcessHashMap.size() == 0) {
            throw new RuntimeException("读取MySQL配置信息失败！");
        }
    }

    /**
     * Phoenix建表
     * @param sinkTable 表名
     * @param sinkColumns 表字段
     * @param sinkPk 表主键
     * @param sinkExtend 表扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // 给主键以及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        // 封装Phoenix建表SQL
        StringBuilder createSQL = new StringBuilder("create table if not exists ").append(GmallConfig.HBASE_SCHEMA)
                .append(".").append(sinkTable).append("(");
        // 遍历添加字段
        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            // 取出字段
            String field = fields[i];
            // 判断当前字段是否为主键
            if (sinkPk.equals(field)) {
                createSQL.append(field).append(" varchar primary key");
            } else {
                createSQL.append(field).append(" varchar");
            }
            // 如果当前字段不是最后一个字段，则追加“,”
            if (i < fields.length - 1) {
                createSQL.append(",");
            }
        }
        createSQL.append(")");
        createSQL.append(sinkExtend);
        log.info(createSQL.toString());

        // 执行建表SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSQL.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建Phoenix表" + sinkTable + "失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
        // 取出数据中的表名和操作类型
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");

        // 使用MaxWell初始化功能时，数据的操作类型为bootstrap-insert
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            jsonObject.put("type", type);
        }

        // 拼接Key
        String key = table + ":" + type;

        // 获取对应Key的TableProcess数据
        TableProcess tableProcess = tableProcessHashMap.get(key);

        // 判断当前配置信息是否存在
        if (tableProcess != null) {
            // 向数据中追加一个sink_table信息
            jsonObject.put("sink_table", tableProcess.getSinkTable());

            // 根据配置信息中提供的数据字段做数据过滤
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());

            // 判断当前数据应该写hbase还是kafka
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                // kafka数据，将数据输出到主流
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                // HBase数据，将数据输出到侧输出流
                context.output(outputTag, jsonObject);
            }
        } else {
            log.info("No key " + key + " In MySQL!");
        }
    }

    /**
     * 根据配置信息中的配置字段过滤数据
     * @param data 原始数据
     * @param sinkColumns 配置信息字段
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        // 保留的数据字段
        String[] fields = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fields);
        Set<Map.Entry<String, Object>> entries = data.entrySet();

        // while (iterator.hasNext()) {
        //     Map.Entry<String, Object> next = iterator.next();
        //     if (!fieldList.contains(next.getKey())) {
        //         iterator.remove();
        //     }
        // }
        entries.removeIf(next -> !fieldList.contains(next.getKey()));
    }
}
