package com.domoes;

import com.domoes.APIs.APIScheduler;
import com.domoes.APIs.ListParams;
import com.domoes.kafka.TaskMQ;
import com.domoes.mongodb.MongoUtils;
import com.domoes.pages.ListPageCrawler;
import com.domoes.utils.ProgramConfig;
import com.domoes.utils.TaskPushUtil;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liufei on 2019/7/15.
 * 以casenumber为关键词抓取文书
 */
public class CaseCrawler {
    private static Logger logger = LoggerFactory.getLogger(Decrypter.class);

    private static Pattern numberPattern;
    private static Set<String> allTemplates = new HashSet<>();              // 保存已获取到的所有案号模板

    public static void main(String[] args) {
        ProgramConfig.parse();

        if (!MongoUtils.init(ProgramConfig.getMongoUri(), ProgramConfig.getMongoDb(), ProgramConfig.getMongoCollection())) {
            logger.error("init mongodb failed");
            return;
        }

        // 只有producer, 用来添加任务
        TaskMQ mqEngine = Driver.createTaskMQ(false, false, false);
        if (mqEngine == null) {
            logger.error("create task MQ failed.");
            return;
        }

        numberPattern = Pattern.compile("(\\d+号)");

        int skip = 0;

        //
        // 先遍历已经获取到的所有文档，把其中的数字编号提取出来{}, 并且记录下哪些数字是已经获取到的，以及最大的数字id
        // 然后从1开始向最大的id，添加那些还没有获取到的casenumber任务
        // 最大的id向上扩展100, 用于添加还未抓取到的文书
        //
        while (!Driver.isStop()) {
            List<Document> docs = MongoUtils.getCaseNumberList(skip, 1000, 30);
            if (docs == null || docs.isEmpty())
                break;

            logger.info("get wenshu casenumber count {}", docs.size());
            skip += docs.size();

            for (Document doc : docs) {
                String casenumber = doc.getString("casenumber");
                if (casenumber != null)
                    casenumber = casenumber.trim();
                if (casenumber == null || casenumber.isEmpty()) {
                    logger.info("casenumber is empty");
                    continue;
                }

                String template = getCaseTemplate(casenumber);
                if (template == null)
                    logger.warn("get case template failed for casenumber {}", casenumber);
                else
                    allTemplates.add(template);
            }
        }

        logger.info("case template set count {}", allTemplates.size());
        // 是否预先拆分查询条件
        boolean preSplitSearchConditions = false;
        // 是否只使用list page任务
        boolean onlyListPage = true;
        // 是否只使用api任务
        boolean onlyApiList = false;

        // 案号模板里包括了 地域、年份、案件类型、法院层级
        // 如果查询结果太多, 只能再通过 审判程序和文书类型进行过滤
        for (String entry : allTemplates) {
            if (onlyApiList || (!onlyListPage && Math.random() > 0.5)) {
                // 以API LIST的形式添加任务
                Map<String, String> params = new HashMap<>();
                params.put("案号", entry);
                if (preSplitSearchConditions) {
                    List<String> result = subdivideRequest(params);
                    for (String item : result) {
                        TaskPushUtil.pushAPIListTask(APIScheduler.encodeValue(item), mqEngine, 1);
                        logger.info("add api list task for casenumber {}, param is {}", entry, params.toString());
                    }
                } else {
                    TaskPushUtil.pushAPIListTask(params, mqEngine, 1);
                    logger.info("add api list task for casenumber {}, param is {}", entry, params.toString());
                }
            } else {
                // 以LIST PAGE的形式添加任务
                List<String> conditions = new ArrayList<>();
                conditions.add("案号:" + entry);
                if (preSplitSearchConditions) {
                    List<List<String>> allConditions = ListParams.subdivideRequestsForCasenumber(conditions);
                    if (allConditions == null) {
                        logger.warn("subdivide conditions failed for case template {}", entry);
                        continue;
                    }
                    for (List<String> cond : allConditions) {
                        String param = ListPageCrawler.combineConditions(cond);
                        String splitUrl = "http://wenshu.court.gov.cn/list/list/?sorttype=1&" + param;
                        TaskPushUtil.pushListPageTask(splitUrl, mqEngine);
                        logger.info("add list page task for casenumber {}, param is {}", entry, param);
                    }
                } else {
                    String param = ListPageCrawler.combineConditions(conditions);
                    String url = "http://wenshu.court.gov.cn/list/list/?sorttype=1&" + param;
                    TaskPushUtil.pushListPageTask(url, mqEngine);
                    logger.info("add list page task for casenumber {}, param is {}", entry, param);
                }
            }
        }

        // 等待线程退出
        Driver.waitStop();
        mqEngine.stop();
    }

    // 获取案号的模板
    // 即将其中的案件编号替换为空, 比如（2013）沪高民一（民）申字第1313号 替换为 （2013）沪高民一（民）申字第
    private static String getCaseTemplate(String casenumber) {
        Matcher numberMatcher = numberPattern.matcher(casenumber);
        if (numberMatcher.find()) {
            String number = numberMatcher.group(1);
            return casenumber.replace(number, "");
        }

        logger.warn("casenumber {} does not match pattern", casenumber);
        return null;
    }

    // 用审判程序和文书类型对查询条件进行拆分
    // 针对API LIST类型
    private static List<String> subdivideRequest(Map<String, String> params) {
        String[] list1 = ListParams.subdivideWenshuTypes(params);
        String[] list2 = ListParams.subdivideJudgePrograms(params);
        List<String> result = new ArrayList<>(list1.length + list2.length);
        Collections.addAll(result, list1);
        Collections.addAll(result, list2);
        return result;
    }
}
