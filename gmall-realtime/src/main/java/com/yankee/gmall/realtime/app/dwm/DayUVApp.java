package com.yankee.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

@Slf4j
public class DayUVApp {
    // 常量配置信息
    private static final String DWD_PAGE_LOG_TOPIC = "dwd_page_log";
    private static final String DWM_PAGE_TOPIC = "dwm_unique_visit";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(2);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://supercluster/gmall/flink/dwm_log/checkpoint"));
        // 设置开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置重启策略，不重启
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 2.读取Kafka
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(DWD_PAGE_LOG_TOPIC, "unique_visit");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // 3.将每行的数据转换成JSON对象
        // SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
        //     @Override
        //     public JSONObject map(String value) throws Exception {
        //         try {
        //             return JSONObject.parseObject(value);
        //         } catch (Exception e) {
        //             e.printStackTrace();
        //             log.info("发现脏数据：" + value);
        //             return null;
        //         }
        //     }
        // });
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context context, Collector<JSONObject> collector) {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty") {
                    }, value);
                }
            }
        });

        // 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream =
                jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // 5.过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new UvRichFilterFunction());

        // 6.写入DWM层Kafka主题中
        // 打印测试
        filterDS.print();
        filterDS.map(JSONObject::toString).addSink(MyKafkaUtil.getKafkaSink(DWM_PAGE_TOPIC));

        // 7.启动任务
        env.execute();
    }

    public static class UvRichFilterFunction extends RichFilterFunction<JSONObject> {
        // 声明状态
        private ValueState<String> firstVisitState;

        // 声明时间格式
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) {
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("visit-state",
                    String.class);

            // 创建状态TTL配置项
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
            stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
            firstVisitState = getRuntimeContext().getState(stringValueStateDescriptor);
        }

        @Override
        public boolean filter(JSONObject value) throws Exception {
            // 取出上一次访问页面
            String lastPageId = value.getJSONObject("page").getString("last_page_id");

            // 判断是否存在上一个页面
            if (lastPageId == null || lastPageId.length() <= 0) {
                // 取出状态数据
                String firstVisitDate = firstVisitState.value();

                // 取出数据时间
                Long ts = value.getLong("ts");
                String curDate = simpleDateFormat.format(ts);

                // 判断
                if (firstVisitDate == null || !firstVisitDate.equals(curDate)) {
                    firstVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
}
