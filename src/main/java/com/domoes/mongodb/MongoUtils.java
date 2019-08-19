package com.domoes.mongodb;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.include;

/**
 * Created by liufei on 2019/6/11.
 * Mongodb操作类
 */
public class MongoUtils {
    private static final Logger logger = LoggerFactory.getLogger(MongoUtils.class);
    private static SimpleDateFormat judgeDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private static MongoCollection<Document> wenshuCollection;      // 文书表
    private static MongoCollection<Document> rawlistCollection;     // 列表原始数据表 (仅保存未成功加载的列表)
    private static MongoCollection<Document> switchIpCollection;    // 切换ip的记录表
    private static MongoCollection<Document> wordsCollection;       // 分词结果表

    public static boolean init(String uri, String database, String collection) {
        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        wenshuCollection = mongoDatabase.getCollection(collection);
        rawlistCollection = mongoDatabase.getCollection("rawlistdata");
        switchIpCollection = mongoDatabase.getCollection("switchip");
        wordsCollection = mongoDatabase.getCollection("words");

        // 索引
        wenshuCollection.createIndex(new Document("casenumber", 1).append("title", 1))
                .subscribe(new SubscriberHelpers.PrintSubscriber<>(logger, "mongodb wenshu index add result {}"));
        wenshuCollection.createIndex(new Document("docid", 1), new IndexOptions().unique(true))
                .subscribe(new SubscriberHelpers.PrintSubscriber<>(logger, "mongodb wenshu index add result {}"));

        switchIpCollection.createIndex(new Document("ip", 1))
                .subscribe(new SubscriberHelpers.PrintSubscriber<>(logger, "mongodb switchip index add result {}"));

        return true;
    }

    // 添加一条新的文书摘要
    // 不检查插入的结果
    // 字段说明
    //    _id
    //    title 标题
    //    casecourt 审判法院
    //    casenumber 文书编号 (唯一索引)
    //    judgedate 审判日期
    //    descdate 获取描述信息的时间
    //    ajlx 案件类型 (有多个类型时用,分隔)
    //    glws 关联文书 (另一个文书的casenumber)
    public static void insertWenshuDesc(String title, String casecourt, String casenumber, String judgedate, String ajlx, String glws, String docId) {
        // judgedate转为日期格式
        Date juedge;
        try {
            juedge = judgeDateFormat.parse(judgedate);
        } catch (Exception e) {
            logger.warn("invalid judgedate format. {}", e.getMessage());
            juedge = new Date();
        }

        // judgedate 为文书的审判时间
        // descdate 为标题采集入库的时间
        // contentdate 为内容采集入库的时间
        Document doc = new Document("title", title)
                .append("casecourt", casecourt)
                .append("casenumber", casenumber)
                .append("judgedate", juedge)
                .append("descdate", new Date())
                .append("ajlx", ajlx)
                .append("glws", glws)
                .append("docid", docId);
        wenshuCollection.insertOne(doc).subscribe(new SubscriberHelpers.OperationSubscriber<>());
    }

    // 添加完整的文书内容
    // 如果docid已存在, 更新content
    public static void insertWenshuFullContent(String title, String casecourt, String casenumber, String judgedate, String ajlx, String glws, String docId,
                                               String content, String caseInfo, int caseType, int courtId) {
        // judgedate转为日期格式
        Date juedge;
        try {
            juedge = judgeDateFormat.parse(judgedate);
        } catch (Exception e) {
            logger.warn("invalid judgedate format. {}", e.getMessage());
            juedge = new Date();
        }

        // 如果该文档已存在, 执行更新操作
        if (docIdExist(docId)) {
            updateWenshuContent(docId, content, caseInfo, caseType, courtId);
        } else {
            Document doc = new Document("title", title)
                    .append("casecourt", casecourt)
                    .append("casenumber", casenumber)
                    .append("judgedate", juedge)
                    .append("descdate", new Date())
                    .append("ajlx", ajlx)
                    .append("glws", glws)
                    .append("docid", docId)
                    .append("content", content)
                    .append("contentdate", new Date())
                    .append("caseinfo", caseInfo)
                    .append("casetype", caseType)
                    .append("courtid", courtId);
            wenshuCollection.insertOne(doc).subscribe(new SubscriberHelpers.OperationSubscriber<>());
        }
    }

    // 更新文书内容
    // 不检查更新结果
    // 字段说明
    //    content 文书内容
    //    contentdate 获取文书内容的时间
    //    docid 文档id
    //    caseinfo 案件原始信息 (json串)
    //    casetype 案件类型编号
    //    courtid 审判法院编号
    public static void updateWenshuContent(String docId, String content,
                                           String caseInfo, int caseType, int courtId) {
        wenshuCollection.updateOne(eq("docid", docId),
                new Document("$set", new Document("content", content).append("contentdate", new Date())
                        .append("caseinfo", caseInfo).append("casetype", caseType).append("courtid", courtId)))
                .subscribe(new SubscriberHelpers.OperationSubscriber<>());
    }

    // 检查指定的文书是否已获取完整
    public static boolean wenshuGotFinished(String docid) {
        SubscriberHelpers.OperationSubscriber<Document> subscriber = new SubscriberHelpers.OperationSubscriber<>();
        wenshuCollection.find(and(eq("docid", docid), exists("content")))
                .projection(include("content"))
                .subscribe(subscriber);

        return checkContentExist(subscriber);
    }

    // 检查指定的文书是否已获取完整
    public static boolean wenshuGotFinished(String casenumber, String title) {
        SubscriberHelpers.OperationSubscriber<Document> subscriber = new SubscriberHelpers.OperationSubscriber<>();
        wenshuCollection.find(and(and(eq("casenumber", casenumber), eq("title", title)), exists("content")))
                .projection(include("content"))
                .subscribe(subscriber);

        return checkContentExist(subscriber);
    }

    private static boolean checkContentExist(SubscriberHelpers.OperationSubscriber<Document> subscriber) {
        try {
            List<Document> contents = subscriber.get(10, TimeUnit.SECONDS);
            if (contents.isEmpty())
                return false;
            Document doc = contents.get(0);
            String content = doc.getString("content");
            return content != null && !content.isEmpty();
        } catch (Throwable e) {
            logger.warn("exception at check wenshu finished. {}", e.getMessage());
            return false;
        }
    }

    // 检查指定的文书是否已获取完整
    public static boolean wenshuGotFinishedByDocId(String docid) {
        SubscriberHelpers.OperationSubscriber<Document> subscriber = new SubscriberHelpers.OperationSubscriber<>();
        wenshuCollection.find(and(eq("docid", docid), exists("content")))
                .projection(include("content"))
                .subscribe(subscriber);

        return checkContentExist(subscriber);
    }

    // 检查指定的docid是否存在
    private static boolean docIdExist(String docid) {
        SubscriberHelpers.OperationSubscriber<Document> subscriber = new SubscriberHelpers.OperationSubscriber<>();
        wenshuCollection.find(eq("docid", docid))
                .projection(include("docid"))
                .subscribe(subscriber);

        try {
            List<Document> docs = subscriber.get(10, TimeUnit.SECONDS);
            return !docs.isEmpty();
        } catch (Throwable e) {
            logger.warn("exception at check docid exist. {}", e.getMessage());
            return false;
        }
    }

    // 通过docId查询案号
//    public static String getCaseNumberByDocId(String docid) {
//        SubscriberHelpers.OperationSubscriber<Document> subscriber = new SubscriberHelpers.OperationSubscriber<>();
//        wenshuCollection.find(eq("docid", docid))
//                .projection(include("casenumber"))
//                .subscribe(subscriber);
//
//        try {
//            List<Document> docs = subscriber.get(10, TimeUnit.SECONDS);
//            if (!docs.isEmpty()) {
//                Document doc = docs.get(0);
//                return doc.getString("casenumber");
//            }
//            logger.warn("can't find document for docId {}", docid);
//            return null;
//        } catch (Throwable e) {
//            logger.warn("exception at get casenumber for docid {}. {}", docid, e.getMessage());
//            return null;
//        }
//    }

    // 获取有内容的文书列表
    public static List<Document> getFinishedWenshuList(int skip, int limit, int waitSeconds) {
        SubscriberHelpers.ObservableSubscriber<Document> subscriber = new SubscriberHelpers.ObservableSubscriber<>();
        wenshuCollection.find(exists("content")).skip(skip).limit(limit).subscribe(subscriber);
        try {
            return subscriber.get(waitSeconds, TimeUnit.SECONDS);
        } catch (Throwable e) {
            logger.warn("exception at get wenshu. {}", e.getMessage());
            return null;
        }
    }

    // 获取文书的casenumber列表
    public static List<Document> getCaseNumberList(int skip, int limit, int waitSeconds) {
        SubscriberHelpers.ObservableSubscriber<Document> subscriber = new SubscriberHelpers.ObservableSubscriber<>();
        wenshuCollection.find(exists("casenumber")).projection(include("casenumber")).skip(skip).limit(limit).subscribe(subscriber);
        try {
            return subscriber.get(waitSeconds, TimeUnit.SECONDS);
        } catch (Throwable e) {
            logger.warn("exception at get wenshu. {}", e.getMessage());
            return null;
        }
    }


    // 添加一条未成功获取的原始列表数据
    public static void insertRawListData(String data, String param, String index) {
        // data 为"[{\"RunEval\":\"w61Zw51
        // param 为该列表的查询条件
        // index 为该列表的查询翻页
        Document doc = new Document("data", data)
                .append("param", param)
                .append("index", index)
                .append("date", new Date());
        rawlistCollection.insertOne(doc).subscribe(new SubscriberHelpers.OperationSubscriber<>());
    }

    public static void deleteRawListData(ObjectId id) {
        rawlistCollection.deleteOne(eq("_id", id))
                .subscribe(new SubscriberHelpers.PrintSubscriber<>(logger, "delete rawlistdata result {}"));
    }

    // 获取rawlistdata表的列表
    public static List<Document> getRawListData(int skip, int limit, int waitSeconds) {
        SubscriberHelpers.ObservableSubscriber<Document> subscriber = new SubscriberHelpers.ObservableSubscriber<>();
        rawlistCollection.find().skip(skip).limit(limit).subscribe(subscriber);
        try {
            return subscriber.get(waitSeconds, TimeUnit.SECONDS);
        } catch (Throwable e) {
            logger.warn("exception at get rawlistdata. {}", e.getMessage());
            return null;
        }
    }


    // 添加一条切换IP的记录
    public static void insertSwitchIpRecord(String ip, String data, String lastIp, String reason, String process) {
        Document doc = new Document("ip", ip)
                .append("lastip", lastIp)
                .append("reason", reason)
                .append("process", process)
                .append("data", data)
                .append("date", new Date());
        switchIpCollection.insertOne(doc).subscribe(new SubscriberHelpers.OperationSubscriber<>());
    }


    // 检查文书的关键词是否已提取
    public static boolean keywordsGotFinished(String docid) {
        SubscriberHelpers.OperationSubscriber<Document> subscriber = new SubscriberHelpers.OperationSubscriber<>();
        wordsCollection.find(and(eq("docid", docid), exists("keywords")))
                .projection(include("keywords"))
                .subscribe(subscriber);

        try {
            List<Document> contents = subscriber.get(10, TimeUnit.SECONDS);
            if (contents.isEmpty())
                return false;
            Document doc = contents.get(0);
            String keywords = doc.getString("keywords");
            return keywords != null && !keywords.isEmpty();
        } catch (Throwable e) {
            logger.warn("exception at check keywords finished. {}", e.getMessage());
            return false;
        }
    }

    // 检查文书的分词是否已完成 (只检查百度分词)
    public static boolean wordSegmentGotFinished(String docid) {
        SubscriberHelpers.OperationSubscriber<Document> subscriber = new SubscriberHelpers.OperationSubscriber<>();
        wordsCollection.find(and(eq("docid", docid), exists("baiduwords")))
                .projection(include("baiduwords"))
                .subscribe(subscriber);

        try {
            List<Document> contents = subscriber.get(10, TimeUnit.SECONDS);
            if (contents.isEmpty())
                return false;
            Document doc = contents.get(0);
            String baiduwords = doc.getString("baiduwords");
            return baiduwords != null && !baiduwords.isEmpty();
        } catch (Throwable e) {
            logger.warn("exception at check baiduwords finished. {}", e.getMessage());
            return false;
        }
    }

    // 获取文书的分词结果
    public static String getSegmentForDoc(String docid) {
        SubscriberHelpers.OperationSubscriber<Document> subscriber = new SubscriberHelpers.OperationSubscriber<>();
        wordsCollection.find(eq("docid", docid))
                .projection(include("baiduwords", "perceptronwords"))
                .subscribe(subscriber);

        try {
            List<Document> contents = subscriber.get(10, TimeUnit.SECONDS);
            if (contents.isEmpty())
                return null;
            Document doc = contents.get(0);
            String baiduwords = doc.getString("baiduwords");
            String perceptronwords = doc.getString("perceptronwords");
            return baiduwords != null && !baiduwords.isEmpty() ? baiduwords : perceptronwords;
        } catch (Throwable e) {
            logger.warn("exception at get doc segment. {}", e.getMessage());
            return null;
        }
    }
}
