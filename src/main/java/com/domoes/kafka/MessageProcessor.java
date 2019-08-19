package com.domoes.kafka;

import com.domoes.APIs.APIScheduler;
import com.domoes.APIs.DocContentCrawler;
import com.domoes.APIs.ListContentCrawler;
import com.domoes.Driver;
import com.domoes.pages.ContentPageCrawler;
import com.domoes.pages.ListPageCrawler;
import com.domoes.pages.NormalSearchCrawler;
import com.domoes.utils.TaskCounter;
import com.domoes.utils.TaskPushUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liufei on 2019/6/9.
 * MessageQueue任务处理
 */
class MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    private static TaskCounter taskCounter = new TaskCounter(100000, -1, 5);

    /**
     * 列表类消息
     * @param message 为list page url
     */
    static void listMessage(TaskMQ mq, String message) {
        try {
            Driver.getDriver().navigate().to(message);
        } catch (Exception e) {
            logger.warn("exception at navigate to {}", message);
            Driver.getDriver().get(message);
        }

        // 刷新一次列表页, 提高页面打开成功的概率
        try { Thread.sleep(100); } catch (Exception e) { logger.warn("exception {}", e.getMessage()); }
        Driver.getDriver().navigate().refresh();

        if (!ListPageCrawler.load(mq)) {
            logger.info("process list page task failed. add task again. {}", message);

            if (taskCounter.incCount(message)) {
                mq.pushMessage(message.getBytes(), TaskMQ.TAG_LIST);
                return;
            }
        }

        // 如果成功或者重试次数超过限制, 删除缓存
        taskCounter.remove(message);
    }

    // 普通搜索类消息
    // message为list page页上的搜索关键字串
    static void normalSearchMessage(TaskMQ mq, String message) {
        NormalSearchCrawler.search(mq, message);
    }

    // 文书类消息
    // message为doc page url
    static void docMessage(TaskMQ mq, String message) {
        try {
            Driver.getDriver().navigate().to(message);
        } catch (Exception e) {
            logger.warn("exception at navigate to {}", message);
            Driver.getDriver().get(message);
        }

        // 刷新一次列表页, 提高页面打开成功的概率
        try { Thread.sleep(100); } catch (Exception e) { logger.warn("exception {}", e.getMessage()); }
        Driver.getDriver().navigate().refresh();

        ContentPageCrawler.load(mq, Driver.getDriver(), null);
    }

    // API列表类消息
    // message为json串 类似{"Param":"xxx", "Index":"1"}
    static void apiListMessage(TaskMQ mq, String message) {
        // 如果任务执行失败, 先检查重试次数是否超过了限制, 如果未超过限制则重新加回到队列
        if (!ListContentCrawler.load(mq, message)) {
            logger.info("process list api message failed. add task again. {}", message);

            if (taskCounter.incCount(message)) {
                mq.pushMessage(message.getBytes(), TaskMQ.TAG_API_LIST);
                return;
            }
        }

        // 如果成功或者重试次数超过限制, 删除缓存
        taskCounter.remove(message);
    }

    // API文档类消息
    // message为docId
    static void apiDocMessage(TaskMQ mq, String message) {
        // 如果任务执行失败, 先检查重试次数是否超过了限制, 如果未超过限制则重新加回到队列
        if (!DocContentCrawler.load(mq, message)) {
            logger.info("process doc api message failed. add task again. {}", message);

            // 失败的任务改成content page任务重新添加回去
            TaskPushUtil.pushContentPageForDoc(message, mq);
        }

        // 如果成功或者重试次数超过限制, 删除缓存
        taskCounter.remove(message);

        // 重定向到列表页
        Driver.getDriver().navigate().to(APIScheduler.cookiePageUrl);
    }
}
