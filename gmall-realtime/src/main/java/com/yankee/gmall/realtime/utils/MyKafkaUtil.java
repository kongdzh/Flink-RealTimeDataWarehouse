package com.yankee.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Kafka工具类
 */
public class MyKafkaUtil {

    private static String KAFKA_SERVER = "hadoop01:9092,hadoop02:9092,hadoop03:9092";
    private static Properties properties = new Properties();
    private static String DEFAULT_TOPIC = "dwd_default_topic";

    static {
        // properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
    }

    /**
     * 获取KafkaSource的方法
     * @param topic 主题
     * @param groupId 消费者组
     * @return FlinkKafkaConsumer
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        // 给配置信息对象添加配置项
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 获取KafkaSource
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    /**
     * 获取KafkaSink的方法
     * @param topic 主题
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    /**
     * 获取KafkaSink的方法
     * @param kafkaSerializationSchema kafka的Schema
     * @param <T> 泛型类型
     * @return
     */
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        // 为了保证一次性语义，必须要保证kafka的事务时间大于checkpoint的提交时间
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "300000");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
