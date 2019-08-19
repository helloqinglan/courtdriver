package com.domoes;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.domoes.kafka.*;
import com.domoes.utils.ProgramConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

class CopyKafkaMQ implements TaskMQ {
    private static final Logger logger = LoggerFactory.getLogger(com.domoes.kafka.KafkaMQ.class);

    private KafkaProducer<Integer, String> producer;
    private KafkaConsumer<Integer, String> consumer;
    private String kafkaTopic;
    private Integer messageKey = 1;

    private Thread consumerThread = null;
    private long consumerThreadWaitMils = 5000;

    boolean init(String kafkaServerAddr, String kafkaTopic, boolean needConsumer, TaskMQ writeMQ) {
        this.kafkaTopic = kafkaTopic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddr);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);

        // consumer
        if (needConsumer) {
            props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddr);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "25");
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1500000");       // 平均每条数据最大处理时间为60秒
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);

            // 启动一个工作线程用于拉消息
            consumerThread = new Thread(() -> {
                consumer.subscribe(Collections.singletonList(kafkaTopic));

                while (!Driver.isStop()) {
                    ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(consumerThreadWaitMils));
                    logger.info("received kafka message, count={}", records.count());

                    for (ConsumerRecord<Integer, String> record : records) {
                        logger.info("received kafka message {}, {} at partition {} offset {}", record.key(), record.value(), record.partition(), record.offset());

                        try {
                            JSONObject tagValue = JSON.parseObject(record.value());
                            String key = tagValue.getString("tag");
                            String value = tagValue.getString("value");
                            logger.info("key={}, value={}", key, value);
                            writeMQ.pushMessage(value.getBytes(), key);
                        } catch (Exception e) {
                            logger.warn("exception at consumer thread. {} - {}", record.value(), e.getMessage());
                        }
                    }
                }
            });
            consumerThread.start();
        }

        return true;
    }

    @Override
    public void pushMessage(byte[] message, String tag) {
        // 将tag和message封装为一个json串
        POJOTagValue messageJson = new POJOTagValue();
        messageJson.setTag(tag);
        messageJson.setValue(new String(message));

        ProducerRecord<Integer, String> msg = new ProducerRecord<>(
                kafkaTopic,
                messageKey++,
                JSON.toJSONString(messageJson)
        );

        producer.send(msg, ((recordMetadata, e) -> {
            if (recordMetadata != null)
                logger.info("message send success, partition {}, offset {}", recordMetadata.partition(), recordMetadata.offset());
            else if (e != null)
                logger.warn("message send failed, {}", e.getMessage());
        }));
    }

    @Override
    public void stop() {
        try {
            if (consumerThread != null)
                consumerThread.wait(consumerThreadWaitMils * 2);
        } catch (Exception e) {
            logger.error("exception in waiting consumer thread quit. {}", e.getMessage());
        }
    }
}

public class TopicCopy {
    private static final Logger logger = LoggerFactory.getLogger(TopicCopy.class);

    public static void main(String[] args) {
        ProgramConfig.parse();

        // 10.0.0.21 写
        CopyKafkaMQ mq21 = new CopyKafkaMQ();
        if (!mq21.init("10.0.0.21:9092", "courtcrawls", false, null)) {
            logger.error("create task MQ 21 failed.");
            return;
        }

        // 10.0.0.18 读
        CopyKafkaMQ mq18 = new CopyKafkaMQ();
        if (!mq18.init("10.0.0.18:9092", "courtcrawls", true, mq21)) {
            logger.error("create task MQ 18 failed.");
            return;
        }

        // 等待线程退出
        Driver.waitStop();
        mq18.stop();
        mq21.stop();
    }
}
