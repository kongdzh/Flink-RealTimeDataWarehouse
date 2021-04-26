package com.yankee.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.yankee.gmall.realtime.app.fun.DBSplitProcessFunction;
import com.yankee.gmall.realtime.app.fun.DimSink;
import com.yankee.gmall.realtime.bean.TableProcess;
import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

@Slf4j
public class DBBaseApp {
    // 常量配置信息
    private static final String ODS_DB_TOPIC = "ods_base_db_m";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，一般情况下和kafka的topic的分区数保持一致
        env.setParallelism(2);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://supercluster/gmall/flink/dwd_db/checkpoint"));
        // 设置开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置重启策略，不重启
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 2.获取Kafka数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(ODS_DB_TOPIC, "dwd_db");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject);

        // 4.过滤掉数据中data为空的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 获取data字段
                String data = value.getString("data");

                return data != null && data.length() > 0;
            }
        });

        // 打印测试
        // filterDS.print();

        // 5.分流，ProcessFunction
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = filterDS.process(new DBSplitProcessFunction(hbaseTag));
        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);
        // 测试打印
        hbaseJsonDS.print("hbase>>>>>");
        kafkaJsonDS.print("kafka>>>>>");

        // 6.取出分流数据，将数据写入Kafka或者Phoenix
        hbaseJsonDS.addSink(new DimSink());
        kafkaJsonDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                log.info("开始序列化Kafka数据！");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sink_table"),
                        element.getJSONObject("data").toString().getBytes());
            }
        }));

        // 7.执行任务
        env.execute();
    }
}
