package com.domoes.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.domoes.Driver;
import org.apache.kafka.clients.consumer.*;
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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by liufei on 2019/6/6.
 * kafka消息队列操作封装
 */
public class KafkaMQ implements TaskMQ {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMQ.class);

    private KafkaProducer<Integer, String> producer;
    private KafkaConsumer<Integer, String> consumer;
    private String kafkaTopic;
    private Integer messageKey = 1;

    private Thread workerThread = null;
    private Thread consumerThread = null;
    private long consumerThreadWaitMils = 5000;

    /**
     * 初始化Kafka producer和consumer
     * @param consumerListener 如果consumerListener为null, 将不会创建consumer
     */
    public boolean init(String kafkaServerAddr, String kafkaTopic, MQListener consumerListener) {
        this.kafkaTopic = kafkaTopic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddr);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);

        // consumer
        if (consumerListener != null) {
            props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddr);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");       // 平均每条数据最大处理时间为60秒
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);

            // 用于暂存任务
            Queue<ConsumerRecord<Integer, String>> taskQueue = new ConcurrentLinkedDeque<>();

            // 启动一个工作线程用于处理消息
            workerThread = new Thread(() -> {
                while (!Driver.isStop()) {
                    ConsumerRecord<Integer, String> record = taskQueue.poll();
                    if (record == null) {
                        logger.info("task queue is empty.");
                        try { Thread.sleep(1000); } catch (Exception e) { logger.info("exception at sleep. {}", e.getMessage()); }
                        continue;
                    }

                    logger.info("received kafka message {}, {} at partition {} offset {}", record.key(), record.value(), record.partition(), record.offset());

                    try {
                        JSONObject tagValue = JSON.parseObject(record.value());
                        String key = tagValue.getString("tag");
                        String value = tagValue.getString("value");
                        logger.info("key={}, value={}", key, value);
                        consumerListener.consume(key, value);
                    } catch (Exception e) {
                        logger.warn("exception at worker thread. {} - {}", record.value(), e.getMessage());

                        // 如果异常是invalid session id, 只能退出重启
                        if (e.getMessage().startsWith("invalid session id")) {
                            Driver.stop();
                            Runtime.getRuntime().exit(0);
                        }
                    }
                }
            });
            workerThread.start();

            // 启动一个工作线程用于拉消息
            consumerThread = new Thread(() -> {
                consumer.subscribe(Collections.singletonList(kafkaTopic));

                while (!Driver.isStop()) {
                    // 临时处理 ------------------------------------
//                Set<TopicPartition> assigns = consumer.assignment();
//                Map<TopicPartition, Long> offsets = consumer.endOffsets(assigns);
//                for (TopicPartition p : assigns) {
//                    long pos = consumer.position(p);
//                    long end = offsets.get(p);
//                    logger.info("partition {} position: {}, end: {}, lag: {}", p.partition(), pos, end, end - pos);
//                    // 跳过一直没更新的老数据
//                    if (p.partition() > 0 && pos < 20000) {
//                        consumer.seek(p, pos + (end - pos) / 3);
//                        consumer.commitAsync();
//                    }
//                }
                    // 临时处理 ------------------------------------

                    long time = System.currentTimeMillis();

                    ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(consumerThreadWaitMils));
                    logger.info("received kafka message, count={}", records.count());

                    // 这里不处理任务, 只加到队列中
                    for (ConsumerRecord<Integer, String> record : records) {
                        taskQueue.add(record);
                    }

                    // 等待消息处理完
                    while (true) {
                        // 如果任务已处理完, 补充新任务
                        if (taskQueue.size() < 3) {
                            logger.info("task queue size is {}, retry get new tasks.", taskQueue.size());
                            break;
                        }

                        // 如果处理的时间太长, 也补充消息, 因为kafka可能要超时
                        long diff = System.currentTimeMillis() - time;
                        if (diff > 500000) {
                            // 如果累积了太多消息就不再再补了, 让kafka超时
                            if (taskQueue.size() > 100) {
                                logger.warn("too many tasks in queue, wait for processing. kafka may timeouted. {}", taskQueue.size());
                                try { Thread.sleep(10000); } catch (Exception e) { logger.info("exception at sleep. {}", e.getMessage()); }
                                continue;
                            }

                            logger.info("elapsed time {} too long, current task queue size is {}. retry get new tasks", diff, taskQueue.size());
                            break;
                        }

                        try { Thread.sleep(1000); } catch (Exception e) { logger.info("exception at sleep. {}", e.getMessage()); }
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
            if (workerThread != null)
                workerThread.wait(consumerThreadWaitMils * 2);
        } catch (Exception e) {
            logger.error("exception in waiting consumer thread quit. {}", e.getMessage());
        }
    }
}
