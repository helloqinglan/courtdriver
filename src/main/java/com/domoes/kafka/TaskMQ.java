package com.domoes.kafka;

/**
 * Created by liufei on 2019/6/6.
 * 用于保存和获取任务的消息队列接口
 */
public interface TaskMQ {
    // Page类型的任务
    String TAG_LIST = "TagList";
    String TAG_NORMAL_SEARCH = "TagNormalSearch";
    String TAG_DOC = "TagDoc";

    // API类型的任务
    String TAG_API_LIST = "TagApiList";
    String TAG_API_DOC = "TagApiDoc";

    // 发送一条消息到任务队列
    void pushMessage(byte[] message, String tag);

    // 结束消息队列的处理过程
    void stop();
}
