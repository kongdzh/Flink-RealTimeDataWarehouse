package com.yankee.gmall.realtime.app.fun;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimJoinFunction<T> {

    /**
     * 获取数据中的外键所要关联维度的主键
     * @param input
     * @return
     */
    String getKey(T input);

    /**
     * 关联事实数据和维度数据
     * @param input
     * @param dimInfo
     * @throws ParseException
     */
    void join(T input, JSONObject dimInfo) throws ParseException;
}
