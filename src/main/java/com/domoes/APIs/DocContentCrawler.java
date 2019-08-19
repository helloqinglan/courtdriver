package com.domoes.APIs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.domoes.mongodb.MongoUtils;
import com.domoes.kafka.TaskMQ;
import org.openqa.selenium.WebDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liufei on 2019/6/30.
 *
 * http://wenshu.court.gov.cn/CreateContentJS/CreateContentJS.aspx?DocID=d8952be5-e5a2-4b8b-b554-cccf5824617f
 * 请求方式：GET
 * Host: wenshu.court.gov.cn
 * X-Requested-With: XMLHttpRequest
 * AJAX请求，主要的请求接口，返回的是列表内容
 *
 */
public class DocContentCrawler {
    private static Logger logger = LoggerFactory.getLogger(DocContentCrawler.class);

    /**
     * 加载指定的文书内容
     * @param docId 文书id
     * @return true表示该任务处理完成, false表示未处理, 调用方会将该任务重新放回消息队列
     */
    public static boolean load(TaskMQ mq, String docId) {
        // 混进了一些无效的docid
        if (!APIScheduler.isValidDocId(docId)) {
            logger.warn("invalid docId {}", docId);
            return true;
        }

        // 文书内容已经获取到
        if (MongoUtils.wenshuGotFinishedByDocId(docId)) {
            logger.info("doc already got. {}", docId);
            return true;
        }

        WebDriver driver = APIScheduler.getDriver();
        String targetUrl = "http://wenshu.court.gov.cn/CreateContentJS/CreateContentJS.aspx?DocID=" + docId;
        driver.navigate().to(targetUrl);

        int loopCount = 0;
        int maxLoopCount = 60;      // 最多尝试次数
        long waitingTime = 500;     // 等待时间 ms

        do {
            String data = driver.getPageSource();
            if (data.isEmpty()) {
                logger.info("waiting for response..., docId={}", docId);
                try { Thread.sleep(waitingTime); } catch (Exception e) { logger.info("exception at sleep. {}", e.getMessage()); }
                continue;
            }

            // 结果是否为有效数据
            if (!data.contains("$(function(){$(\"#con_llcs\").html")) {
                logger.warn("invalid result. {}", data);

                if (data.contains("window.location.href='/Html_Pages/VisitRemind20180914.html")) {
                    // 如果没有走代理, 并且是显示要输入验证码, 等待输入成功后重新打开该文书页
                    if (!APIScheduler.isProxyMode() && !APIScheduler.isHeadless()) {
                        driver.navigate().to("http://wenshu.court.gov.cn/Html_Pages/VisitRemind20180914.html?DocID=" + docId);

                        int i = 0;
                        for (; i < 60; i++) {
                            String url = driver.getCurrentUrl();
                            if (!url.startsWith("http://wenshu.court.gov.cn/Html_Pages/VisitRemind20180914.html?DocID="))
                                break;
                            try {
                                Thread.sleep(1000);
                            } catch (Exception e) {
                                logger.warn("exception. {}", e.getMessage());
                            }
                        }

                        if (i == 60)
                            break;

                        driver.navigate().to(targetUrl);
                        continue;
                    } else {
                        logger.info("need visit verify page.");
                        return false;
                    }
                }

                // 访问太频繁, 等一会儿
                if (data.contains("https://www.abuyun.com/")) {
                    logger.info("abuyun visit too frequent, wait a minute");
                    break;
                }

                String errorMsg = pageErrorMessage(driver.getTitle(), data);
                if (errorMsg != null)
                    APIScheduler.refresh(errorMsg);
                break;
            }

            data = data.replace("<html><head></head><body>", "");
            data = data.replace("</body></html>", "");

            logger.debug("data is {}", data);

            // 找第一段: caseinfo
            int begin = data.indexOf("stringify({");
            if (begin < 0) {
                logger.warn("can't find stringify({");
                break;
            }
            logger.info("find caseinfo start index {}", begin);
            int end = data.indexOf("});", begin);
            if (end < 0) {
                logger.warn("can't find caseinfo end.");
                break;
            }
            logger.info("find caseinfo end index {}", end);
            String caseinfoScript = data.substring(begin + 10, end + 1);        // 去掉前面的stringify(和后面的);, 只留{} json数据
            logger.debug("caseinfo script: {}", caseinfoScript);

            JSONObject caseInfo;
            try {
                caseInfo = JSON.parseObject(caseinfoScript);
            } catch (Exception e) {
                logger.warn("invalid caseinfo json data. {}", e.getMessage());
                break;
            }
            String caseTypeStr = caseInfo.getString("案件类型");
            int caseType = 0;
            try {
                caseType = Integer.parseInt(caseTypeStr);
            } catch (Exception e) {
                logger.warn("get case type failed. {}", e.getMessage());
            }
            String courtIDStr = caseInfo.getString("法院ID");
            int courtID = 0;
            try {
                courtID = Integer.parseInt(courtIDStr);
            } catch (Exception e) {
                logger.warn("get court id failed. {}", e.getMessage());
            }

            // 找第二段 content
            begin = data.indexOf("\\\"Html\\\":\\\"");
            if (begin < 0) {
                logger.warn("can't find jsonHtmlData");
                break;
            }
            logger.info("find jsonHtmlData start index {}", begin);
            end = data.indexOf("\\\"}\";", begin);
            if (end < 0) {
                logger.warn("can't find jsonHtmlData end.");
                break;
            }
            logger.info("find jsonHtmlData end index {}", end);
            String content = data.substring(begin + 11, end);        // 去掉前面的\"Html\":\"
            logger.debug("jsonHtmlData script: {}", content);

            MongoUtils.updateWenshuContent(docId, content, caseinfoScript, caseType, courtID);
            return true;
        } while (++loopCount < maxLoopCount);

        logger.warn("get doc content failed, docId={}", docId);
        return false;
    }

    // 检查网页内容里是否有要求刷新网页的提示
    private static String pageErrorMessage(String title, String data) {
        if (title.contains("访问验证") || data.contains("访问验证") || data.contains("window.location.href='/Html_Pages/VisitRemind20180914.html"))
            return "doc page need verification";
        if (data.contains("请开启JavaScript并刷新该页"))
            return "doc page javascript error, need refresh";
        return null;
    }
}
