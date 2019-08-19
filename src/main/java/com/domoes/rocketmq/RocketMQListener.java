package com.domoes.rocketmq;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.domoes.kafka.MQListener;
import com.domoes.kafka.TaskMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liufei on 2019/6/3.
 * RocketMQ任务处理
 */
public class RocketMQListener extends MQListener implements MessageListener {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQListener.class);

    public RocketMQListener(TaskMQ mq, boolean ignoreListTask, boolean ignoreApiTask) {
        super(mq, ignoreListTask, ignoreApiTask);
    }

    public Action consume(Message message, ConsumeContext context) {
        logger.info("Receive RocketMQ message: {}", message);
        String tag = message.getTag();
        String body = new String(message.getBody());

        consume(tag, body);
        return Action.CommitMessage;
    }
}
