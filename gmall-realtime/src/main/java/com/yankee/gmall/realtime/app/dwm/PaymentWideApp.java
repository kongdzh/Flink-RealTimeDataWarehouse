package com.yankee.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.yankee.gmall.realtime.bean.OrderWide;
import com.yankee.gmall.realtime.bean.PaymentInfo;
import com.yankee.gmall.realtime.bean.PaymentWide;
import com.yankee.gmall.realtime.utils.DateTimeUtil;
import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

@Slf4j
public class PaymentWideApp {
    // 常量配置信息
    private static final String DWD_PAYMENT_INFO_TOPIC = "dwd_payment_info";
    private static final String DWM_ORDER_WIDE_TOPIC = "dwm_order_wide";
    private static final String DWM_PAYMENT_WIDE_TOPIC = "dwm_payment_wide";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://supercluster/gmall/flink/dwm_db/checkpoint"));
        // 开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置重启策略：不重启
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 2.获取KafkaSource
        DataStreamSource<String> paymentInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(DWD_PAYMENT_INFO_TOPIC,
                "payment_wide"));
        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(DWM_ORDER_WIDE_TOPIC,
                "payment_wide"));

        // 3.将数据转换为JavaBean，并提取时间戳生成watermark
        // 4.按照OrderID分组
        WatermarkStrategy<PaymentInfo> paymentInfoWatermarkStrategy =
                WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                });
        KeyedStream<PaymentInfo, Long> paymentInfoWithOrderIDKeyedStream = paymentInfoKafkaDS.map(data -> {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            // 将JSON字符串装换为JavaBean
            PaymentInfo paymentInfo = JSONObject.parseObject(data, PaymentInfo.class);

            // 取出create_time字段
            // String create_time = paymentInfo.getCreate_time();

            // 设置create_ts
            // paymentInfo.setCreate_ts(simpleDateFormat.parse(create_time).getTime());

            paymentInfo.setCreate_ts(DateTimeUtil.toTs(paymentInfo.getCreate_time()));

            return paymentInfo;
        }).assignTimestampsAndWatermarks(paymentInfoWatermarkStrategy).keyBy(PaymentInfo::getOrder_id);

        KeyedStream<OrderWide, Long> orderWideWithOrderIDKeyedStream =
                orderWideKafkaDS.map(data -> JSONObject.parseObject(data, OrderWide.class))
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                        try {
                                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd " +
                                                    "HH:mm:ss");
                                            return simpleDateFormat.parse(element.getCreate_time()).getTime();
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                            throw new RuntimeException("时间格式错误！");
                                        }
                                    }
                                })).keyBy(OrderWide::getOrder_id);

        // 5.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoWithOrderIDKeyedStream
                .intervalJoin(orderWideWithOrderIDKeyedStream)
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context,
                                               Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        // 打印测试
        paymentWideDS.print("PaymentWide>>>>>>");

        // 6.将数据写入Kafka
        paymentWideDS.map(JSONObject::toJSONString).addSink(MyKafkaUtil.getKafkaSink(DWM_PAYMENT_WIDE_TOPIC));

        // 7.提交任务
        env.execute();
    }
}
