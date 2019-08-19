package com.domoes.rocketmq;

import com.aliyun.openservices.ons.api.*;
import com.domoes.kafka.TaskMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

/**
 * Created by liufei on 2019/6/3.
 * RocketMQ操作封装
 */
public class RocketMQ implements TaskMQ {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQ.class);
    private Producer producer = null;

    private String aliOnsTopic = null;

    // 初始化
    public boolean init(String aliAccessKey, String aliSecretKey, String aliOnsAddr, String aliOnsTopic,
                        String aliGroupID, String aliOnsConsumerTag, MessageListener consumerListener) {
        this.aliOnsTopic = aliOnsTopic;

        Properties properties = new Properties();
        properties.put(PropertyKeyConst.AccessKey, aliAccessKey);
        properties.put(PropertyKeyConst.SecretKey, aliSecretKey);
        // 设置发送超时时间，单位毫秒
        properties.setProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        properties.put(PropertyKeyConst.NAMESRV_ADDR, aliOnsAddr);

        ONSFactory.createProducer(properties);
        producer.start();

        // consumer
        properties.put(PropertyKeyConst.GROUP_ID, aliGroupID);
        // 集群订阅方式 (默认)
        properties.put(PropertyKeyConst.MessageModel, PropertyValueConst.CLUSTERING);
        Consumer consumer = ONSFactory.createConsumer(properties);

        consumer.subscribe(aliOnsTopic, aliOnsConsumerTag, consumerListener);

        return true;
    }

    // 发送一条消息到RocketMQ
    @Override
    public void pushMessage(byte[] message, String tag) {
        Message msg = new Message( //
                // Message 所属的 Topic
                aliOnsTopic,

                // Message Tag 可理解为 Gmail 中的标签，对消息进行再归类，方便 Consumer 指定过滤条件在消息队列 RocketMQ 的服务器过滤
                tag,

                // Message Body 可以是任何二进制形式的数据， 消息队列 RocketMQ 不做任何干预，
                // 需要 Producer 与 Consumer 协商好一致的序列化和反序列化方式
                message);

        // 异步发送消息
        producer.sendAsync(msg, new SendCallback() {
            @Override
            public void onSuccess(final SendResult sendResult) {
                logger.info("{} Send mq message success. Topic is: {}, msgId is {}", new Date(), msg.getTopic(), sendResult.getMessageId());
            }

            @Override
            public void onException(OnExceptionContext context) {
                // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
                logger.warn("{} Send mq message failed. Topic is: {}, exception: {}", new Date(), msg.getTopic(), context.getException());
            }
        });
    }

    @Override
    public void stop() {
    }
}
