package com.yankee.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.List;

/**
 * Redis：
 * 1.存什么数据？            维度数据：JsonStr
 * 2.用什么类型？            String Set Hash
 * 3.RedisKey的设计？       String：tableName+id    Set：tableName   Hash：tableName
 * tableName:ID:NAME -> DIM_USER_INFO:19:zhangsan
 * <p>
 * 集合方式排除，原因在于我们需要对每条独立的维度数据设置过期时间
 */
@Slf4j
public class DimUtil {
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnValues) {
        if (columnValues.length <= 0) {
            throw new RuntimeException("查询维度数据时，请至少设置一个查询条件！");
        }

        // 创建Phoenix Where 子句
        StringBuilder whereSql = new StringBuilder(" WHERE ");

        // 创建RedisKey
        StringBuilder redisKey = new StringBuilder(tableName).append(":");

        // 遍历查询条件并赋值whereSql
        for (int i = 0; i < columnValues.length; i++) {
            // 获取单个查询条件
            Tuple2<String, String> columnValue = columnValues[i];

            // 获取查询条件的名称以及值
            String column = columnValue._1;
            String value = columnValue._2;
            whereSql.append(column).append(" = '").append(value).append("'");

            // 拼接RedisKey
            redisKey.append(value);

            // 如果不是最后一个查询条件，则添加“and”
            // 如果redisKey不是最后一个，添加":"
            if (i < columnValues.length - 1) {
                whereSql.append(" and ");
                redisKey.append(":");
            }
        }

        // 拼接SQL
        String querySql = "SELECT * FROM " + tableName + whereSql;

        // 获取Redis连接
        Jedis jedis = RedisUtil.getJedis();
        String dimJsonStr = jedis.get(redisKey.toString());

        // 查询是否从Redis中查询到数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            jedis.close();
            return JSONObject.parseObject(dimJsonStr);
        }

        // 打印Phoenix SQL
        log.info("Phoenix SQL>>>>>" + querySql);
        // 打印RedisKey
        log.info("Redis Key>>>>>" + redisKey);

        // 查询Phoenix中的维度数据
        List<JSONObject> queryList = PhoenixUtil.queryList(querySql, JSONObject.class);
        JSONObject dimJsonObj = queryList.get(0);

        // 将数据写入Redis
        jedis.set(redisKey.toString(), dimJsonObj.toString());
        jedis.expire(redisKey.toString(), 24 * 60 * 60);
        jedis.close();

        return dimJsonObj;
    }

    public static JSONObject getDimInfo(String tableName, String value) {
        return getDimInfo(tableName, new Tuple2<>("ID", value));
    }

    public static void deleteCached(String tableName, String id) {
        String key = tableName.toUpperCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("缓存异常！");
        }
    }
}
