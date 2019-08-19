package com.domoes.utils;

import com.domoes.APIs.APIScheduler;
import com.domoes.APIs.ListParams;
import com.domoes.kafka.TaskMQ;

import java.util.Map;

/**
 * Created by liufei on 2019/7/3.
 * 用于添加任务的辅助类
 */
public class TaskPushUtil {
    // 添加API LIST类型的任务
    // 注意: 该接口是以json参数的方式进行添加, 不是添加url
    public static void pushAPIListTask(Map<String, String> params, TaskMQ mq, int index) {
        String paramStr = APIScheduler.encodeValue(ListParams.joinParamMaps(params));
        pushAPIListTask(paramStr, mq, index);
    }
    public static void pushAPIListTask(String param, TaskMQ mq, int index) {
        String json = String.format("{'Param': '%s', 'Index':'%d'}", param, index);
        mq.pushMessage(json.getBytes(), TaskMQ.TAG_API_LIST);
    }

    // 添加API DOC类型的任务
    // docId: a34b1ced-9c80-4509-b353-0588bbb88b5e
//    public static void pushAPIDocTask(String docId, TaskMQ mq) {
//        mq.pushMessage(docId.getBytes(), TaskMQ.TAG_API_DOC);
//    }

    // 添加LIST PAGE类型的任务
    // url: http://wenshu.court.gov.cn/List/List?sorttype=1&conditions=searchWord+4+AJLX++案件类型:赔偿案件
    public static void pushListPageTask(String url, TaskMQ mq) {
        mq.pushMessage(url.getBytes(), TaskMQ.TAG_LIST);
    }

    // 添加Normal Search类型的任务
//    private static void pushNormalSearchTask(String search, TaskMQ mq) {
//        mq.pushMessage(search.getBytes(), TaskMQ.TAG_NORMAL_SEARCH);
//    }

    // 添加doc Page类型的任务
    // url: http://wenshu.court.gov.cn/content/content?DocID=a34b1ced-9c80-4509-b353-0588bbb88b5e&KeyWord=
    private static void pushDocPageTask(String url, TaskMQ mq) {
        mq.pushMessage(url.getBytes(), TaskMQ.TAG_DOC);
    }

    // 添加指定案号(docid)的CONTENT PAGE任务
    // docid: a34b1ced-9c80-4509-b353-0588bbb88b5e
    public static void pushContentPageForDoc(String docid, TaskMQ mq) {
        String url = "http://wenshu.court.gov.cn/content/content?DocID=%s&KeyWord=";
        url = String.format(url, docid);
        pushDocPageTask(url, mq);
    }

    // 添加LIST PAGE任务, 指定搜索某一个或某一类案件编号的任务
    // casenumber: （2013）深南法知民初字第918-920号
//    private static boolean pushListPageForCase(String casenumber, TaskMQ mq) {
//        String url = "http://wenshu.court.gov.cn/list/list/?sorttype=1&conditions=searchWord+案号+++案号:" + casenumber;
//        pushListPageTask(url, mq);
//        return true;
//    }

    // 添加指定全文搜索的LIST PAGE任务
    // word: 全文搜索的关键字
    public static void pushFullTextListPage(String word, TaskMQ mq) {
        String url = "http://wenshu.court.gov.cn/list/list/?sorttype=1&conditions=searchWord+QWJS+++全文检索:" + word;
        pushListPageTask(url, mq);
    }
}
