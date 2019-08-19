package com.domoes.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liufei on 2019/6/28.
 * 抽象的消息队列处理类
 */
public class MQListener {
    private static Logger logger = LoggerFactory.getLogger(MQListener.class);
    private TaskMQ mq;
    private boolean ignoreListTask;
    private boolean ignoreApiTask;

    public MQListener(TaskMQ mq, boolean ignoreListTask, boolean ignoreApiTask) {
        this.mq = mq;
        this.ignoreListTask = ignoreListTask;
        this.ignoreApiTask = ignoreApiTask;
    }

    protected void consume(String tag, String body) {
        switch (tag) {
            case TaskMQ.TAG_LIST:
                if (ignoreListTask)
                    mq.pushMessage(body.getBytes(), tag);
                else
                    MessageProcessor.listMessage(mq, body);
                break;

            case TaskMQ.TAG_NORMAL_SEARCH:
                MessageProcessor.normalSearchMessage(mq, body);
                break;

            case TaskMQ.TAG_DOC:
                MessageProcessor.docMessage(mq, body);
                break;

            case TaskMQ.TAG_API_LIST:
                if (ignoreListTask || ignoreApiTask)
                    mq.pushMessage(body.getBytes(), tag);
                else
                    MessageProcessor.apiListMessage(mq, body);
                break;

            case TaskMQ.TAG_API_DOC:
                if (ignoreApiTask)
                    mq.pushMessage(body.getBytes(), tag);
                else
                    MessageProcessor.apiDocMessage(mq, body);
                break;

            default:
                logger.warn("undefined message, tag {}, body {}", tag, body);
                break;
        }
    }
}
