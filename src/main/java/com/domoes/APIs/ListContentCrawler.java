package com.domoes.APIs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.domoes.mongodb.MongoUtils;
import com.domoes.kafka.TaskMQ;
import com.domoes.utils.TaskPushUtil;
import okhttp3.*;
import org.openqa.selenium.JavascriptExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Created by liufei on 2019/6/22.
 *
 * http://wenshu.court.gov.cn/List/ListContent
 * 请求方式：POST
 * Host: wenshu.court.gov.cn
 * X-Requested-With: XMLHttpRequest
 * AJAX请求，主要的请求接口，返回的是列表内容
 *
 */
public class ListContentCrawler {
    private static Logger logger = LoggerFactory.getLogger(ListContentCrawler.class);
    private static int totalListCount = 0;

    /**
     * 通过API方式加载指定的列表页内容
     * @param message json结构体, 包含字段Param, Index
     * @return true表示该任务处理完成, false表示未处理, 调用方会将该任务重新放回消息队列
     */
    public static boolean load(TaskMQ mq, String message) {
        String param, index;
        try {
            JSONObject json = JSON.parseObject(message);
            param = json.getString("Param");
            index = json.getString("Index");
            logger.debug("Param is {} Index is {}", param, index);
        } catch (Exception e) {
            logger.error("parse json message failed. {}", e.getMessage());
            return true;
        }

        OkHttpClient client = APIScheduler.getOkHttpClient();
        JavascriptExecutor jsExecutor = APIScheduler.getJsExecutor();

        String order = "";
        try {
            order = APIScheduler.encodeValue("法院层级");
            if (order == null)
                order = "";
            logger.debug("order is {}", order);
        } catch (Exception e) {
            logger.warn("exception. {}", e.getMessage());
        }

        String guid = APIScheduler.getGUID();
        String vl5x = APIScheduler.getVl5x();
        if (guid == null || vl5x == null) {
            logger.warn("APIScheduler is not initialized. {} {}", guid, vl5x);
            APIScheduler.refresh("APIScheduler is not initialized.");
            return false;
        }

        RequestBody formBody = new FormBody.Builder(Charset.forName("UTF-8"))
                .addEncoded("Param", param)
                .addEncoded("Index", index)
                .addEncoded("Page", "10")
                .addEncoded("Order", order)
                .addEncoded("Direction", "asc")
                .addEncoded("number", "wens")
                .addEncoded("guid", guid)
                .addEncoded("vl5x", vl5x)
                .build();
        logger.debug("form body is {}", formBody.toString());

        String targetUrl = "http://wenshu.court.gov.cn/List/ListContent";
        Request request = new Request.Builder()
                .url(targetUrl)
                .header("Cookie", APIScheduler.getCookie())
                .addHeader("X-Requested-With", "XMLHttpRequest")
                .addHeader("Accept-Encoding", "gzip, deflate")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.9")
                .addHeader("Host", "wenshu.court.gov.cn")
                .addHeader("Origin", "http://wenshu.court.gov.cn")
                .addHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36")
                .post(formBody)
                .build();
        logger.debug("request is {}", request.toString());

        totalListCount = 0;
        try (Response response = client.newCall(request).execute()) {
            logger.debug("response code {}", response.code());
            if (!response.isSuccessful() || response.body() == null) {
                logger.error("post failed. {}", response);
                // code=429, message=Too Many Requests, url=http://wenshu.court.gov.cn/waf_verify.htm
                // code=503, message=Service Unavailable
                // code=502, bad gateway
                // 不需要刷新网页, 访问频率太快, 等一下就好了
                return false;
            }

            String data = response.body().string();
            if (data.startsWith("<html>") || data.startsWith("<!DOCTYPE html>")) {
                logger.warn("invalid result. {}, headers {}", data, response.headers().toString());
                String errorMsg = pageErrorMessage("", data);
                if (errorMsg != null)
                    APIScheduler.refresh(errorMsg);
                return false;
            }

            // 需要重新计算本地key
            if (data.equalsIgnoreCase("\"remind\"") || data.equalsIgnoreCase("\"remind key\"")) {
                logger.warn("invalid key, data is {}", data);
                APIScheduler.refresh("list page remind key, page need refresh");
                return false;
            }

            logger.debug("data is {}", data);
            if (!decryptData(jsExecutor, mq, data, param, index, true, false)) {
                MongoUtils.insertRawListData(data, APIScheduler.decodeValue(param), index);
            }
        } catch (Exception e) {
            logger.warn("exception at call url. {}", e.getMessage());
            return false;
        }

        // 如果当前为第1页, 并且文书数量超过了10条, 生成后面最多19页的请求
        if (totalListCount > 10 && index.equals("1")) {
            String decodedParams = APIScheduler.decodeValue(param);
            logger.info("try to generate next pages request for {}", decodedParams);

            int count = Math.min(20, (totalListCount + 9) / 10);
            for (int page = 2; page < count; page++) {
                TaskPushUtil.pushAPIListTask(param, mq, page);
            }
        }

        // 如果当前为第1页, 并且该查询条件下的文书数量超过了200条, 拆分查询条件
        if (totalListCount > 200 && index.equals("1")) {
            String decodedParams = APIScheduler.decodeValue(param);
            logger.info("try to subdivide request for {}", decodedParams);

            String[] splitParams = ListParams.subdivideRequests(decodedParams);
            if (splitParams != null) {
                for (String item : splitParams) {
                    String encodedItem = APIScheduler.encodeValue(item);
                    // 只构造第一页的查询, 如果有翻页会在处理该请求时再生成
                    TaskPushUtil.pushAPIListTask(encodedItem, mq, 1);
                    logger.info("add new list task. {}", item);
                }
            }
        }

        return true;
    }

    // 检查网页内容里是否有要求刷新网页的提示
    private static String pageErrorMessage(String title, String data) {
        if (title.contains("访问验证") || data.contains("访问验证") || data.contains("window.location.href='/Html_Pages/VisitRemind20180914.html"))
            return "list page need verification";
        if (data.contains("请开启JavaScript并刷新该页"))
            return "list page javascript error, need refresh";
        return null;
    }

    // 解密列表数据并添加doc任务
    // 参数refresh表示当出现解析错误时是否需要刷新网页 (在Decrypter中不需要刷新)
    // 参数check表示是否在检查模式下, 在该模式下如果count为空也返回true (不用再继续检查)
    public static boolean decryptData(JavascriptExecutor jsExecutor, TaskMQ mq, String data, String param, String index, boolean refresh, boolean check) {
        Object result;
        try {
            data = "(" +  data + ")";
            String script = "datalist = eval(" + data + ");dataCount = (datalist[0].Count != undefined ? datalist[0].Count : 0);if (datalist[0].RunEval != undefined) {eval(unzip(datalist[0].RunEval));} return datalist;";
            result = jsExecutor.executeScript(script);
        } catch (Exception e) {
            logger.warn("exception at execute {}. {}, {}", data, e.getMessage(), e.getCause());
            // 如果是js错误, 需要重新加载网页
            //if (refresh && e.getMessage().contains("javascript error"))
            //    APIScheduler.refresh("list page javascript error, eval failed");
            return false;
        }

        try {
            if (result instanceof List) {
                List listData = (List)result;

                // 该查询条件下的文书总数量
                String countStr = (String)((Map)listData.get(0)).get("Count");
                if (countStr == null) {
                    // 没有返回结果 (在check模式下返回true, 正常抓取模式下返回false)
                    // 结果为空就是系统繁忙, 有时候重新请求就能有返回, 有时候可能就是被封IP了, 无法通过一次行为来做判断
                    logger.warn("list api result is empty. Param: {}, Index {}", APIScheduler.decodeValue(param), index);
                    return check;
                } else if (countStr.equalsIgnoreCase("0")) {
                    // 该查询条件没有数据
                    logger.info("there is no result for this search param {} index {}, countStr {}", APIScheduler.decodeValue(param), index, countStr);
                    return true;
                }

                totalListCount = Integer.parseInt(countStr);
                logger.info("total list count {}", totalListCount);
                logger.info("doc count is {}", listData.size() - 1);

                for (int i = 1; i < listData.size(); i++) {
                    Map item = (Map)listData.get(i);
                    String docIdStr = (String)item.get("文书ID");
                    String title = (String)item.get("案件名称");
                    String program = (String)item.get("审判程序");
                    String casetype = (String)item.get("案件类型");
                    String caseNumber = (String)item.get("案号");
                    String caseCourt = (String)item.get("法院名称");
                    String judgeDate = (String)item.get("裁判日期");
                    casetype = combineCaseType(casetype, program);

                    String script = "unzipId = unzip('" + docIdStr + "');return com.str.Decrypt(unzipId);";
                    Object docIdRes;
                    try {
                        logger.info("try to decrypt docId {}", docIdStr);
                        docIdRes = jsExecutor.executeScript(script);
                        logger.info("decrypted docId is {}", docIdRes);
                    } catch (Exception e) {
                        logger.warn("exception at execute decrypt docId. {} \n {}", e.getMessage(), script);
                        // 如果是UTF-8错误, 不需要再尝试, 这些都解不出来, 放到原始数据里
                        if (e.getMessage().contains("javascript error: Malformed UTF-8 data")) {
                            logger.info("incorrect UTF-8 data, ignore.");
                            return false;
                        }
                        // 如果是js错误, 需要重新加载网页
                        if (refresh && e.getMessage().contains("javascript error")) {
                            APIScheduler.refresh("list page javascript error, decrypt docid failed");
                            return false;
                        }
                        continue;
                    }
                    if (docIdRes instanceof String) {
                        String docId = (String) docIdRes;
                        logger.info("docId={}, title={}", docId, title);
                        if (docId.isEmpty()) {
                            logger.warn("docId is empty. {}", docId);
                            continue;
                        }
                        if (!APIScheduler.isValidDocId(docId)) {
                            logger.warn("invalid docid {}", docId);
                            continue;
                        }

                        MongoUtils.insertWenshuDesc(title, caseCourt, caseNumber, judgeDate, casetype, null, docId);
                        if (mq != null && !MongoUtils.wenshuGotFinished(docId))
                            mq.pushMessage(docId.getBytes(), TaskMQ.TAG_API_DOC);
                    } else
                        logger.warn("decrypt doc id failed. {}", docIdRes);
                }
            } else
                logger.warn("invalid javascript result data. {}", result);
        } catch (Exception e) {
            logger.warn("exception at get data. {}， {}", e.getMessage(), e.getCause());
            return false;
        }

        return true;
    }

    // 根据案件类型编号和审判程序, 合成字符串形式的案件类型 (参考官网前端js的实现)
    private static String combineCaseType(String caseType, String program) {
        caseType = convertCaseType(caseType);
        switch (program) {
            case "再审审查与审判监督":
                return caseType + ", 审监";
            case "非诉执行审查":
                return caseType + ", 非诉";
            case "刑罚变更":
                return "";
            case "其他":
                return caseType;
            case "执行":
                if (caseType.equalsIgnoreCase("执行"))
                    return "执行";
                return caseType + ", 执行";
        }
        return caseType;
    }

    private static String convertCaseType(String caseType) {
        if (caseType.matches("\\d+")) {
            switch (caseType) {
                case "1":
                    return "刑事";

                case "2":
                    return "民事";

                case "3":
                    return "行政";

                case "4":
                    return "赔偿";

                case "5":
                    return "执行";

                case "6":
                    return "知产";
            }
        }

        return caseType;
    }
}
