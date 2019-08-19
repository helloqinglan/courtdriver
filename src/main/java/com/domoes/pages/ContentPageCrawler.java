package com.domoes.pages;

import com.domoes.APIs.APIScheduler;
import com.domoes.mongodb.MongoUtils;
import com.domoes.kafka.TaskMQ;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by liufei on 2019/6/15.
 * 抓取文书页
 * http://wenshu.court.gov.cn/content/content?DocID=a34b1ced-9c80-4509-b353-0588bbb88b5e&KeyWord=
 */
public class ContentPageCrawler {
    private static final Logger logger = LoggerFactory.getLogger(ContentPageCrawler.class);
    private static final int waitingForDivContent = 3;     // 秒

    /**
     * 使用网页方式抓取文书页
     * @param itemDescs 列表页上的文书信息 (可选)
     * @return true: 表示获取成功, 或者不需要重试的系统错误
     *         false: 在列表页会刷新该文书, 重试最多3次
     */
    public static boolean load(TaskMQ mq, WebDriver driver, Map<String, Map<String, String>> itemDescs) {
        String url;
        try {
            url = driver.getCurrentUrl();
            logger.info("parse wenshu for url {}", url);
        } catch (Exception e) {
            logger.warn("exception at wenshu page crawler. {}", e.getMessage());
            return true;
        }

        // 记录网页url, 里面有docid
        // 如果网页打开失败, 可以把url记下来, 添加一个新的任务

        // 拿到docid后访问下面这个地址
        // http://wenshu.court.gov.cn/CreateContentJS/CreateContentJS.aspx?DocID=d8952be5-e5a2-4b8b-b554-cccf5824617f
        // 能够更快速的获取到文书内容
        String docId = getDocIdFromUrl(url);
        if (docId == null || !APIScheduler.isValidDocId(docId)) {
            logger.warn("can't find docId in url. {}", url);
            return true;
        }
        logger.info("try to get page content for docid {}", docId);

        if (MongoUtils.wenshuGotFinished(docId)) {
            logger.info("doc {} already got finished.", docId);
            return true;
        }

        if (checkDocRemind(driver, mq) || isSystemError(driver)) {
            logger.info("system error, add new doc task for docId {}", docId);
            mq.pushMessage(docId.getBytes(), TaskMQ.TAG_API_DOC);
            return true;
        }

        // 获取隐藏段信息
        String caseNumber = "";      // hidCaseNumber
        String caseInfo = "";        // hidCaseInfo
        int caseType = 0;            // hidCaseType
        int courtId = 0;             // HidCourtID
        String caseTitle = "";       // hidCaseName
        String caseCourt = "";       // hidCourt
                                     // hidPageType
                                     // hidRequireLogin

        int failedTimes = 0;
        int maxTryTimes = 20;        // 最多尝试次数
        do {
            try {
                WebDriverWait wait = new WebDriverWait(driver, waitingForDivContent);
                WebElement hiddenElement = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("hidDocID")));
                String hidDocId = hiddenElement.getAttribute("value");
                if (!hidDocId.equalsIgnoreCase(docId)) {
                    logger.error("docId in url is not same as in content. {} - {}", docId, hidDocId);
                }

                hiddenElement = driver.findElement(By.id("hidCaseName"));
                caseTitle = hiddenElement.getAttribute("value");

                hiddenElement = driver.findElement(By.id("hidCourt"));
                caseCourt = hiddenElement.getAttribute("value");

                hiddenElement = driver.findElement(By.id("hidCaseNumber"));
                caseNumber = hiddenElement.getAttribute("value");

                hiddenElement = driver.findElement(By.id("hidCaseInfo"));
                caseInfo = hiddenElement.getAttribute("value");

                hiddenElement = driver.findElement(By.id("hidCaseType"));
                String value = hiddenElement.getAttribute("value");
                try {
                    // 有的文档中案件类型是中文名字
                    caseType = Integer.parseInt(value);
                } catch (Exception e) {
                    caseType = 0;
                }

                hiddenElement = driver.findElement(By.id("HidCourtID"));
                value = hiddenElement.getAttribute("value");
                courtId = Integer.parseInt(value);

                break;
            } catch (Exception e) {
                logger.info("exception at get hidden elements. {}", e.getMessage());

                if (checkDocRemind(driver, mq) || isSystemError(driver)) {
                    logger.info("get hidden court info failed, add new doc task for docId {}", docId);
                    mq.pushMessage(docId.getBytes(), TaskMQ.TAG_API_DOC);
                    return true;
                }
            }
        } while (++failedTimes < maxTryTimes);

        if (failedTimes >= maxTryTimes || caseNumber.isEmpty()) {
            logger.warn("can't get casenumber for document {}", docId);
            mq.pushMessage(docId.getBytes(), TaskMQ.TAG_API_DOC);
            return false;
        }
        logger.info("doc casenumber is {}, url {}", caseNumber, url);

        Map<String, String> info = null;
        if (itemDescs != null)
            info = itemDescs.get(caseNumber);
        if (info == null) {
            logger.warn("can't find case desc for casenumber {}", caseNumber);
        }

        // 每个div内的数据作为一个单独的元素保存
        List<String> contents = new ArrayList<>();

        failedTimes = 0;
        maxTryTimes = 40;        // 最多尝试次数
        do {
            try {
                WebDriverWait wait = new WebDriverWait(driver, waitingForDivContent);
                WebElement mainDiv = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("DivContent")));

                List<WebElement> contentsDiv = mainDiv.findElements(By.tagName("div"));
                logger.debug("page div count is {}", contentsDiv.size());
                for (WebElement element : contentsDiv) {
                    String innerData = element.getAttribute("innerHTML");
                    // 过滤掉html
                    if (innerData != null) {
                        innerData = innerData.replaceAll("<[^>]*>", "");
                        if (!isEmptyContent(innerData)) {
                            logger.debug("div content: {}", innerData);
                            contents.add(innerData);
                        }
                    }
                }

                // 内容是否加载完成
                if (!contents.isEmpty())
                    break;

                try { Thread.sleep(500); } catch (Exception e) { logger.warn("exception {}", e.getMessage()); }
            } catch (Exception e) {
                logger.warn("exception at get DivContent. {}", e.getMessage());

                if (checkDocRemind(driver, mq) || isSystemError(driver))
                    break;
            }
        } while (++failedTimes < maxTryTimes);

        // 入库
        if (!contents.isEmpty()) {
            logger.info("update doc content for docid {}", docId);
            String allContent = String.join("\n", contents);
            String title = info != null ? info.get("title") : caseTitle;
            String court = info != null ? info.get("casecourt") : caseCourt;
            String judgedate = info != null ? info.get("judgedate") : "1970-01-01";
            MongoUtils.insertWenshuFullContent(title, court, caseNumber, judgedate,
                    info != null ? info.get("ajlx") : "", info != null ? info.get("glws") : "",
                    docId, allContent, caseInfo, caseType, courtId);
        } else if (mq != null) {
            // 如果没有获取到内容, 添加一条doc任务
            logger.info("doc content is empty, add new doc task for docId {}", docId);
            mq.pushMessage(docId.getBytes(), TaskMQ.TAG_API_DOC);
        }

        return true;
    }

    private static String getDocIdFromUrl(String url) {
        int begin = url.indexOf("DocID=");
        if (begin < 0) {
            logger.warn("have not find DocID in url {}", url);
            return null;
        }
        int end = url.indexOf("&", begin);
        if (end < 0)
            return url.substring(begin + 6);
        return url.substring(begin + 6, end);   // 去掉DocID=
    }

    private static boolean checkDocRemind(WebDriver driver, TaskMQ mq) {
        String url = driver.getCurrentUrl();
        if (url.contains("VisitRemind20180914.html")) {
            logger.info("doc page need verification. add task and ignore. {}", url);
            String docId = getDocIdFromUrl(url);
            if (docId != null && mq != null)
                mq.pushMessage(docId.getBytes(), TaskMQ.TAG_API_DOC);
            return true;
        }

        return false;
    }

    // 检查该网页是否为系统错误
    // 系统错误的网页忽略, 也不需要刷新, 因为url不正确, 刷新也没有用
    // http://wenshu.court.gov.cn/Html_Pages/Html500Error.html?aspxerrorpath=/content/content
    private static boolean isSystemError(WebDriver driver) {
        String title = "";
        try {
            title = driver.getTitle();
            return title != null && (title.contains("500错误") || title.contains("503 Service Unavailable")
                    || title.contains("服务器不能访问") || title.contains("Method not found")
                    || title.contains("502 - Web 服务器在作为网关或代理服务器时收到了无效响应。"));
        } catch (Exception e) {
            logger.error("catch exception at check system error. title: {}, message: {}", title, e.getMessage());
            return true;
        }
    }

    // 内容是否为空
    private static boolean isEmptyContent(String content) {
        if (content == null || content.isEmpty())
            return true;

        content = content.replaceAll("\\s+", "");
        return content.isEmpty();
    }
}
