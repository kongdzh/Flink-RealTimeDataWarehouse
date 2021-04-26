package com.yankee.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

@Slf4j
public class UserJumpDetailApp {
    // 常量配置信息
    private static final String DWD_PAGE_LOG_TOPIC = "dwd_page_log";
    private static final String DWM_PAGE_TOPIC = "dwm_user_jump_detail";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://supercluster/gmall/flink/dwm_log/checkpoint"));
        // 开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置重启策略：不重启
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 2.读取Kafka主题数据创建流
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(DWD_PAGE_LOG_TOPIC, "user_jump_detail");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // 测试
        // DataStream<String> kafkaDS = env.fromElements(
        //         "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":1000000000} ",
        //         "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":1000000001} ",
        //         "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"home\"}," +
        //                 "\"ts\":1000000020} ",
        //         "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"goog_list\",\"last_page_id\":\"detail\"}," +
        //                 "\"ts\":1000000025} "
        // );
        // DataStreamSource<String> kafkaDS = env.socketTextStream("hadoop01", 9999);

        // 测试：提取数据中的时间戳生成watermark
        // 老版本：默认使用的处理时间语义
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 新版本：默认时间语义为事件时间
        // WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
        //     @Override
        //     public long extractTimestamp(JSONObject element, long recordTimestamp) {
        //         return element.getLong("ts") * 1000L;
        //     }
        // });

        // 测试
        // SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
        //     @Override
        //     public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        //         JSONObject jsonObject = null;
        //         try {
        //             jsonObject = JSONObject.parseObject(value);
        //             collector.collect(jsonObject);
        //         } catch (Exception e) {
        //             context.output(new OutputTag<String>("dirty") {
        //             }, value);
        //         }
        //     }
        // }).assignTimestampsAndWatermarks(watermarkStrategy);

        // 3.数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = null;
                try {
                    jsonObject = JSONObject.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty") {
                    }, value);
                }
            }
        });

        // 4.按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream =
                jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // 5.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出上一跳页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).followedBy("follow").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出上一跳页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId != null && lastPageId.length() > 0;
            }
        }).within(Time.seconds(10));

        // 6.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // 7.提取事件和超时事件
        OutputTag<String> timeOutTag = new OutputTag<String>("TimeOut") {
        };
        SingleOutputStreamOperator<Object> selectDS = patternStream.flatSelect(timeOutTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp,
                                Collector<String> collector) {
                collector.collect(pattern.get("start").get(0).toString());
            }
        }, new PatternFlatSelectFunction<JSONObject, Object>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<Object> collector) {
                // 什么都不写，因为匹配上的数据是不需要的数据
            }
        });

        // 打印测试
        selectDS.getSideOutput(timeOutTag).print();

        // 8.将数据写入Kafka
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(DWM_PAGE_TOPIC);
        selectDS.getSideOutput(timeOutTag).addSink(kafkaSink);

        // 9.执行任务
        env.execute();
    }
}
