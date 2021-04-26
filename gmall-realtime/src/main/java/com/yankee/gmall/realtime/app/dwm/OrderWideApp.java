package com.yankee.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.yankee.gmall.realtime.app.fun.DimAsyncFunction;
import com.yankee.gmall.realtime.bean.OrderDetail;
import com.yankee.gmall.realtime.bean.OrderInfo;
import com.yankee.gmall.realtime.bean.OrderWide;
import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    // 常量配置信息
    private static final String DWD_ORDER_INFO_TOPIC = "dwd_order_info";
    private static final String DWD_ORDER_DETAIL_TOPIC = "dwd_order_detail";
    private static final String DWM_ORDER_WIDE_TOPIC = "dwm_order_wide";

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

        // 2.读取Kafka订单和订单明细主题数据
        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil.getKafkaSource(DWD_ORDER_INFO_TOPIC,
                "order_wide");
        FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtil.getKafkaSource(DWD_ORDER_DETAIL_TOPIC,
                "order_wide");
        DataStreamSource<String> orderInfoKafkaDS = env.addSource(orderInfoKafkaSource);
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(orderDetailKafkaSource);

        // 3.将每行数据转换为JavaBean，提取时间戳生成watermark
        WatermarkStrategy<OrderInfo> orderInfoWatermarkStrategy = WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                });
        WatermarkStrategy<OrderDetail> orderDetailWatermarkStrategy = WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                });
        KeyedStream<OrderInfo, Long> orderInfoWithIdKeyedStream = orderInfoKafkaDS.map(data -> {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            // 将JSON字符串转换为JavaBean
            OrderInfo orderInfo = JSONObject.parseObject(data, OrderInfo.class);

            // 取出create_time字段
            String create_time = orderInfo.getCreate_time();

            // 按照空格分割
            String[] createTimeArr = create_time.split(" ");

            orderInfo.setCreate_date(createTimeArr[0]);
            orderInfo.setCreate_hour(createTimeArr[1]);
            orderInfo.setCreate_ts(simpleDateFormat.parse(create_time).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(orderInfoWatermarkStrategy)
                .keyBy(OrderInfo::getId);

        KeyedStream<OrderDetail, Long> orderDetailWithOrderIdKeyedStream = orderDetailKafkaDS.map(data -> {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            // 将JSON字符串转换为JavaBean
            OrderDetail orderDetail = JSONObject.parseObject(data, OrderDetail.class);

            // 取出create_time字段
            String create_time = orderDetail.getCreate_time();
            orderDetail.setCreate_ts(simpleDateFormat.parse(create_time).getTime());

            return orderDetail;
        }).assignTimestampsAndWatermarks(orderDetailWatermarkStrategy)
                .keyBy(OrderDetail::getOrder_id);

        // 4.双流Join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithIdKeyedStream
                .intervalJoin(orderDetailWithOrderIdKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))// 生产环境，为了不丢数据，设置为最大的网络延迟
                // 默认是闭区间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context,
                                               Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        // 测试打印
        // orderWideDS.print("OrderWideDS>>>>>>>>");

        // 5.关联维度信息
        // 5.1 关联用户维度信息
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        // 取出用户维度中的生日
                        String birthday = dimInfo.getString("BIRTHDAY");
                        // 当前系统时间对应的毫秒数
                        long currentTs = System.currentTimeMillis();
                        // 生日对应的毫秒数
                        long ts = simpleDateFormat.parse(birthday).getTime();

                        // 获取年龄
                        long ageLong = (currentTs - ts) / 1000 / 60 / 60 / 24 / 365;
                        // 将年龄字段赋值给客户订单宽表
                        orderWide.setUser_age((int) ageLong);

                        // 获取性别
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));
                    }
                }, 60, TimeUnit.SECONDS);
        // 用户维度关联测试
        // orderWideWithUserDS.print("OrderWideWithUserDS>>>>>>");

        // 5.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS =
                AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        // 提取维度信息并设置进OrderWide
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS);
        // 区域维度相关测试
        // orderWideWithProvinceDS.print("OrderWideWithProvinceDS>>>>>>");

        // 5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSKUDS =
                AsyncDataStream.unorderedWait(orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                        orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS);
        // SKU维度打印测试
        // orderWideWithSKUDS.print("OrderWideWithSKUDS>>>>>>");

        // 5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSPUDS = AsyncDataStream.unorderedWait(orderWideWithSKUDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 5.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTMDS = AsyncDataStream.unorderedWait(orderWideWithSPUDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 5.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategoryDS =
                AsyncDataStream.unorderedWait(orderWideWithTMDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return orderWide.getCategory3_id().toString();
                            }

                            @Override
                            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                                orderWide.setCategory3_name(dimInfo.getString("NAME"));
                            }
                        }, 60,
                        TimeUnit.SECONDS);
        // 打印测试
        orderWideWithCategoryDS.print("OrderWide>>>>>>");

        // 6.写入数据到Kafka
        orderWideWithCategoryDS.map(JSONObject::toJSONString).addSink(MyKafkaUtil.getKafkaSink(DWM_ORDER_WIDE_TOPIC));

        // 7.开启任务
        env.execute();
    }
}
