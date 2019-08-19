package com.domoes;

import com.domoes.APIs.APIScheduler;
import com.domoes.APIs.ListContentCrawler;
import com.domoes.kafka.TaskMQ;
import com.domoes.mongodb.MongoUtils;
import com.domoes.utils.ProgramConfig;
import com.domoes.utils.TaskCounter;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by liufei on 2019/7/3.
 * DocId解码入口
 */
public class Decrypter {
    private static Logger logger = LoggerFactory.getLogger(Decrypter.class);
    private static TaskCounter taskCounter = new TaskCounter(100000, 30, 2);

    public static void main(String[] args) {
        ProgramConfig.parse();

        if (!MongoUtils.init(ProgramConfig.getMongoUri(), ProgramConfig.getMongoDb(), ProgramConfig.getMongoCollection())) {
            logger.error("init mongodb failed");
            return;
        }

        if (!APIScheduler.init(ProgramConfig.isUseProxy(), ProgramConfig.isUseRemoteDriver(), ProgramConfig.isHeadless(),
                ProgramConfig.getProxyIpAndPort(), ProgramConfig.getRateLimit(), ProgramConfig.isSwitchIp())) {
            logger.error("params init failed.");
            APIScheduler.stop();
            return;
        }

        // 只有producer, 用来添加任务
        TaskMQ mqEngine = Driver.createTaskMQ(false, false, false);
        if (mqEngine == null) {
            logger.error("create task MQ failed.");
            return;
        }

        WebDriver driver = APIScheduler.getDriver();
        JavascriptExecutor jsExecutor = APIScheduler.getJsExecutor();

        // 测试能否执行
        String testData = "[{\"RunEval\":\"w61aXW/CgjAUw70tGB9KWMO2B8KIT8O+woQ9w54QYsOQTR4mS2VPw4bDvz5gw5jCtVAFBy1VTkLCrsKBw57Cj3PDjsKlNzRxwrlPwrfCu2PDgsOTwq98w7XClsOzw7TDsMOxw7rDjsKzw4/DtX7Dg8OXw5l2w4cCL8KgCMOGacOzAhFgw6bDscKGPAPCkcKZbFdMJcO0FgbCukNZGHQPworCocK5wqAOw77CkAPChMOBGsOCQARoAiXCoA50AjlQQgdgw7AiOGjDvDDCjFfCiyQ7HHPDvsKdw6QZX8KEMcOFUXExFsKcw47CpcKbwrhOZ8KffiPCqTBRFDDCr1rDsEnDjVkudMKvVMK3w4vCv38nECsfFx7DpMKVKxRRw7HDkyjCqMK6e3XDgMO1Yg3DnFrDn8KbwpjDpGzDusOnwqJUwoXCui1RC8KCwpbDg8KNw5wCZD9FwqXDm07DmD09w65xwrvDm8O3fwEDwqLChsKGwo4SP17CksKRM8K5w4hOw51nYj8Zwq1kNsKxQcKBHiPDp8KlwptXR8K3ScOhTcKUwpAGwrQtBlPDrAgTIsOaAmvCrMOYwoPDjsKfJyJQF8KbZgvCqggmG2gKDMKyPRNEw43Di3fDu2h1w63Ct8KxwrvDrETCmMKsw7Rxwr4fJVN2w5UNADXCisOmHMOtdUB2Ui/DjXnCuEXCjsKUwrnCpX8qJ8KkASd6wofDhAMewqnCrMOPw7zDsAc=\",\"Count\":\"0\"},]";
        boolean result;
        do {
            result = ListContentCrawler.decryptData(jsExecutor, null, testData, "", "", false, true);
            if (!result) {
                logger.warn("test execute javascript failed. refresh webdriver.");
                driver.get(APIScheduler.cookiePageUrl);
                try { Thread.sleep(2000); } catch (Exception e) { logger.warn("exception. {}", e.getMessage()); }
            }
        } while (!result);

        int skip = 0;

        while (!Driver.isStop()) {
            List<Document> docs = MongoUtils.getRawListData(skip, 10, 30);
            if (docs == null || docs.isEmpty()) {
                try { Thread.sleep(10000); } catch (Exception e) { logger.warn("exception. {}", e.getMessage()); }
                skip = 0;
                continue;
            }

            logger.info("get rawlistdata count {}", docs.size());
            skip += docs.size();

            for (Document doc : docs) {
                String data = doc.getString("data");
                String param = doc.getString("param");
                String index = doc.getString("index");
                ObjectId id = doc.getObjectId("_id");
                logger.info("id {}", id);

                // 执行成功则从mongodb中将该条数据删除
                if (data.equalsIgnoreCase("\"remind key\"") || data.equalsIgnoreCase("\"remind\"") ||
                        ListContentCrawler.decryptData(jsExecutor, mqEngine, data, param, index, false, true)) {
                    logger.info("invalid docId or id {} get succeed, delete it.", id);
                    MongoUtils.deleteRawListData(id);
                    taskCounter.remove(id.toString());
                } else {
                    logger.info("id {} get failed again.", id);
                    // 重试次数超过限制了也删除
                    if (!taskCounter.incCount(id.toString())) {
                        logger.warn("task {} retried too many times, ignore this data.", id);
                        MongoUtils.deleteRawListData(id);
                        taskCounter.remove(id.toString());
                    }
                }

                try { Thread.sleep(1000); } catch (Exception e) { logger.warn("exception. {}", e.getMessage()); }
            }
        }

        // 等待线程退出
        Driver.waitStop();
        mqEngine.stop();
    }
}
