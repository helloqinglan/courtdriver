package com.domoes;

import com.domoes.APIs.APIScheduler;
import com.domoes.kafka.KafkaMQ;
import com.domoes.kafka.MQListener;
import com.domoes.kafka.TaskMQ;
import com.domoes.mongodb.MongoUtils;
import com.domoes.utils.*;
import org.openqa.selenium.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liufei on 2019/5/31.
 * 入口
 */
public class Driver {
    private static final Logger logger = LoggerFactory.getLogger(Driver.class);
    public static final long waitSecondsForElement = 1;                 // driver异步等待Element可用的超时时间
    public static final int maxPageForList = 20;                        // 列表页最大页数

    private static volatile boolean stop = false;

    public static void main(String[] args) {
        ProgramConfig.parse();

        if (!MongoUtils.init(ProgramConfig.getMongoUri(), ProgramConfig.getMongoDb(), ProgramConfig.getMongoCollection())) {
            logger.error("init mongodb failed");
            return;
        }

        if (!APIScheduler.init(ProgramConfig.isUseProxy(), ProgramConfig.isUseRemoteDriver(), ProgramConfig.isHeadless(),
                ProgramConfig.getProxyIpAndPort(), ProgramConfig.getRateLimit(), ProgramConfig.isSwitchIp())) {
            logger.error("params init failed.");
            //APIScheduler.stop();
            return;
        }

        // 获取消息队列里的任务
        TaskMQ mqEngine = createTaskMQ(true, ProgramConfig.isIgnoreListTask(), ProgramConfig.isIgnoreApiTask());
        if (mqEngine == null) {
            logger.error("create task MQ failed.");
            return;
        }

        // 等待线程退出
        waitStop();
        mqEngine.stop();
    }

    static void waitStop() {
        while (!stop) {
            try {
                // 每隔5分钟检查一次要不要更换ip
                Thread.sleep(5 * 60 * 1000);
                APIScheduler.checkRefresh();
            } catch (Exception e) {
                logger.warn("exception at thread sleep. {}", e.getMessage());
            }
        }
    }

    // 停止服务
    public static void stop() {
        stop = true;
    }

    public static boolean isStop() {
        return stop;
    }

    public static WebDriver getDriver() {
        return APIScheduler.getDriver();
    }

    // 改为只支持kafka消息队列 (阿里的消息队列太贵, 暂时不可能用到)
    // needConsumer表示是否需要消费kafka消息, 如果不需要则只会创建producer
    // ignoreListTask 表示是否忽略掉List类型的任务 (当没有consumer时该参数无意义)
    static TaskMQ createTaskMQ(boolean needConsumer, boolean ignoreListTask, boolean ignoreApiTask) {
        KafkaMQ engine = new KafkaMQ();
        MQListener listener = null;
        if (needConsumer)
            listener = new MQListener(engine, ignoreListTask, ignoreApiTask);

        if (!engine.init(ProgramConfig.getKafkaServerAddr(), ProgramConfig.getKafkaTopic(), listener)) {
            logger.error("KafkaMQ initialized failed");
            return null;
        }

        return engine;
    }
}
