package com.domoes;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.domoes.kafka.TaskMQ;
import com.domoes.mongodb.MongoUtils;
import com.domoes.services.SegmentRequest;
import com.domoes.services.SegmentResponse;
import com.domoes.utils.HanLPClient;
import com.domoes.utils.ProgramConfig;
import com.domoes.utils.TaskPushUtil;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by liufei on 2019/7/4.
 * 分词
 */
public class WordSplitter {
    private static Logger logger = LoggerFactory.getLogger(WordSplitter.class);

    private static Map<String, Integer> topWords = new HashMap<>();

    public static void main(String[] args) {
        ProgramConfig.parse();
        HanLPClient hanLPClient = new HanLPClient(ProgramConfig.getHanlpServiceAddr(), ProgramConfig.getHanlpServicePort());

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

        int skip = 0;

        while (!Driver.isStop()) {
            List<Document> docs = MongoUtils.getFinishedWenshuList(skip, 10, 30);
            if (docs == null || docs.isEmpty()) {
                //try { Thread.sleep(10000); } catch (Exception e) { logger.warn("exception at sleep. {}", e.getMessage()); }
                //skip = 0;
                //continue;
                break;
            }

            logger.info("get finished wenshu count {}", docs.size());
            skip += docs.size();

            for (Document doc : docs) {
                String content = doc.getString("content");
                if (content == null || content.isEmpty()) {
                    logger.info("content is empty");
                    continue;
                }

                // 过滤掉html (html标签都替换成换行, 用于下面的关键词提取)
                content = content.replaceAll("<[^>]*>", "\n").replaceAll("(\\n){2,}", "\n");
                logger.info("content length {}", content.length());

                // 把一些固定用语中间的空格去掉
                content = content.replaceAll("决 定 书", "决定书")
                        .replaceAll("行 政 裁 定 书", "行政裁定书")
                        .replaceAll("审 判 长", "审判长")
                        .replaceAll("审 判 员", "审判员")
                        .replaceAll("书 记 员", "书记员");

                ObjectId id = doc.getObjectId("_id");
                String docid = doc.getString("docid");
                String title = doc.getString("title");
                //String casecourt = doc.getString("casecourt");
                //String casenumber = doc.getString("casenumber");
                //String ajlx = doc.getString("ajlx");
                String caseInfo = doc.getString("caseinfo");
                //int casetype = doc.getInteger("casetype");
                //int courtid = doc.getInteger("courtid");
                logger.info("id {} docid {}", id, docid);

                // 提取关键词
                if (!MongoUtils.keywordsGotFinished(docid))
                    extractKeywords(title, caseInfo, content);

                String wordsResponse;
                // 百度分词是否已完成
                if (MongoUtils.wordSegmentGotFinished(docid)) {
                    wordsResponse = MongoUtils.getSegmentForDoc(docid);
                } else {
                    int method = SegmentRequest.Method.Perceptron_VALUE | SegmentRequest.Method.BaiduNLP_VALUE;
                    SegmentResponse words = hanLPClient.segment(docid, content, method);
                    if (words == null) {
                        logger.warn("segment response is null for docid {}", docid);
                        continue;
                    }
                    //logger.info("words: \n{} \n{}", words.getPerceptronValue(), words.getBaiduValue());
                    wordsResponse = words.getBaiduValue() != null ? words.getBaiduValue() : words.getPerceptronValue();
                }

                if (wordsResponse == null) {
                    logger.warn("have not got words for docid {}", docid);
                    continue;
                }

                try {
                    JSONArray json = JSONObject.parseArray(wordsResponse);
                    for (int i = 0; i < json.size(); i++) {
                        JSONObject obj = json.getJSONObject(i);
                        String word = obj.getString("word");
                        Integer value = topWords.putIfAbsent(word, 1);
                        if (value != null) {
                            topWords.put(word, value + 1);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("exception at parse json. {}, {}", e.getMessage(), wordsResponse);
                }
            }
        }

        // 当某个词出现次数超过1000时加入到任务队列
        logger.info("words total count {}", topWords.size());
        for (Map.Entry<String, Integer> entry : topWords.entrySet()) {
            if (entry.getKey().length() >= 2 && entry.getValue() > 1000) {
                logger.info("add word {} to list page task.", entry.getKey());
                TaskPushUtil.pushFullTextListPage(entry.getKey(), mqEngine);
            }
        }

        // 等待线程退出
        Driver.waitStop();
        mqEngine.stop();
    }

    // 提取关键词
    // 规则: 一行文本中以:分隔的两部分, 前半部分为key, 后半部分为value
    //       另外有一些特定的词也是关键词, 比如 书记员 某某
    //      文本内容不能太长, 只有在几个字以内的才是关键词
    // 然后再对剩下的文本做分词
    private static void extractKeywords(String title, String caseInfo, String content) {
        String[] setences = content.split("\\n");
        for (String item : setences) {
            item = item.replaceAll("\\s+", "");
            String[] segs = item.split("：");
        }
    }
}
