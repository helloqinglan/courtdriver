package com.domoes.APIs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by liufei on 2019/6/30.
 * List接口Param参数支持的关键字类型
 */
public class ListParams {
    private static Logger logger = LoggerFactory.getLogger(ListParams.class);
/*
    enum ParamType {
        FullText,           // 全文检索
        CaseName,           // 案件名称
        CourtName,          // 法院名称
        CaseType,           // 案件类型
        WenshuType,         // 文书类型
        JudgeOfficer,       // 审判人员
        LawersOffice,       // 律所
        CaseReason,         // 案由
        CaseNumber,         // 案号
        CourtLayer,         // 法院层级
        JudgeProgram,       // 审判程序
        JudgeDate,          // 审判日期
        Person,             // 当事人
        Lawyer,             // 律师
        LegalBasis,         // 法律依据
        JudgeYear,          // 裁判年份
        Keyword,            // 关键词
        Header,             // 首部
        Truth,              // 事实
        Reason,             // 理由
        JudgeResult,        // 判决结果
        Footer,             // 尾部
        CourtProvince,      // 法院地域
    }
*/
    static String[] caseTypes = new String[] {"刑事案件", "民事案件", "行政案件", "赔偿案件", "执行案件"};
    private static String[] judgeYears = new String[] {"1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003",
            "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"};
    private static String[] courtLayers = new String[] {"最高法院", "高级法院", "中级法院", "基层法院"};
    public static String[] judgePrograms = new String[] {"一审", "二审", "再审", "复核", "刑罚变更", "非诉执行审查", "再审审查与审判监督", "其他"};
    public static String[] wenshuTypes = new String[] {"判决书", "裁定书", "调解书", "决定书", "通知书", "批复", "答复", "函", "令", "其他"};
    private static String[] courtProvinces = new String[] {"北京市", "天津市", "河北省", "山西省", "内蒙古自治区", "辽宁省",
            "吉林省", "黑龙江省", "上海市", "江苏省", "浙江省", "安徽省", "福建省", "江西省", "山东省", "河南省", "湖北省", "湖南省",
            "广东省", "广西壮族自治区", "海南省", "重庆市", "四川省", "贵州省", "云南省", "西藏自治区", "陕西省", "甘肃省", "青海省",
            "宁夏回族自治区", "新疆维吾尔自治区"};

    // 将请求进行细分
    // 细分的规则:
    //    各条件的优先级: 案件类型 --> 文书类型 --> 裁判年份 --> 法院层级 --> 审判程序 --> 法院名称 --> 关键词(1,2,3,...)
    static String[] subdivideRequests(String param) {
        Map<String, String> paramPairs = splitParams(param);
        if (!paramPairs.containsKey("文书类型"))
            return subdivideWenshuTypes(paramPairs);
        if (!paramPairs.containsKey("审判程序"))
            return subdivideJudgePrograms(paramPairs);
        if (!paramPairs.containsKey("案件类型"))
            return subdivideCaseTypes(paramPairs);
        if (!paramPairs.containsKey("裁判年份"))
            return subdivideJudgeYears(paramPairs);
        if (!paramPairs.containsKey("法院层级"))
            return subdivideCourtLayers(paramPairs);
        if (!paramPairs.containsKey("法院地域"))
            return subdivideCourtProvinces(paramPairs);

        logger.warn("cannot split request params. {}", param);
        return null;
    }

    public static List<List<String>> subdivideRequests(List<String> conditions) {
        Set<String> params = new HashSet<>();
        for (String item : conditions) {
            String[] datas = item.split(":");
            if (datas.length != 2) {
                logger.warn("invalid condition {}", item);
                continue;
            }
            params.add(datas[0]);
        }

        if (!params.contains("文书类型")) {
            List<List<String>> result = new ArrayList<>(wenshuTypes.length);
            for (String item : wenshuTypes) {
                List<String> data = new ArrayList<>(conditions);
                data.add("文书类型:" + item);
                result.add(data);
            }
            return result;
        }
        if (!params.contains("审判程序")) {
            List<List<String>> result = new ArrayList<>(judgePrograms.length);
            for (String item : judgePrograms) {
                List<String> data = new ArrayList<>(conditions);
                data.add("审判程序:" + item);
                result.add(data);
            }
            return result;
        }
        if (!params.contains("案件类型")) {
            List<List<String>> result = new ArrayList<>(caseTypes.length);
            for (String item : caseTypes) {
                List<String> data = new ArrayList<>(conditions);
                data.add("案件类型:" + item);
                result.add(data);
            }
            return result;
        }
        if (!params.contains("裁判年份")) {
            List<List<String>> result = new ArrayList<>(judgeYears.length);
            for (String item : judgeYears) {
                List<String> data = new ArrayList<>(conditions);
                data.add("裁判年份:" + item);
                result.add(data);
            }
            return result;
        }
        if (!params.contains("法院层级")) {
            List<List<String>> result = new ArrayList<>(courtLayers.length);
            for (String item : courtLayers) {
                List<String> data = new ArrayList<>(conditions);
                data.add("法院层级:" + item);
                result.add(data);
            }
            return result;
        }
        if (!params.contains("法院地域")) {
            List<List<String>> result = new ArrayList<>(courtProvinces.length);
            for (String item : courtProvinces) {
                List<String> data = new ArrayList<>(conditions);
                data.add("法院地域:" + item);
                result.add(data);
            }
            return result;
        }

        logger.warn("cannot split request params.");
        return null;
    }

    // 用审判程序和文书类型对查询条件进行拆分
    // 针对LIST PAGE类型
    public static List<List<String>> subdivideRequestsForCasenumber(List<String> conditions) {
        List<List<String>> result = new ArrayList<>(ListParams.wenshuTypes.length + ListParams.judgePrograms.length);

        for (String item : ListParams.wenshuTypes) {
            List<String> data = new ArrayList<>(conditions);
            data.add("文书类型:" + item);
            result.add(data);
        }

        for (String item : ListParams.judgePrograms) {
            List<String> data = new ArrayList<>(conditions);
            data.add("审判程序:" + item);
            result.add(data);
        }

        return result;
    }

    // 细分案件类型
    static String[] subdivideCaseTypes(Map<String, String> params) {
        List<String> result = new ArrayList<>(caseTypes.length);
        for (String item : caseTypes) {
            params.put("案件类型", item);
            String data = joinParamMaps(params);
            params.remove("案件类型");
            result.add(data);
        }

        return result.toArray(new String[0]);
    }

    // 细分文书类型
    public static String[] subdivideWenshuTypes(Map<String, String> params) {
        List<String> result = new ArrayList<>(wenshuTypes.length);
        for (String item : wenshuTypes) {
            params.put("文书类型", item);
            String data = joinParamMaps(params);
            params.remove("文书类型");
            result.add(data);
        }

        return result.toArray(new String[0]);
    }

    // 细分裁判年份
    private static String[] subdivideJudgeYears(Map<String, String> params) {
        List<String> result = new ArrayList<>(judgeYears.length);
        for (String item : judgeYears) {
            params.put("裁判年份", item);
            String data = joinParamMaps(params);
            params.remove("裁判年份");
            result.add(data);
        }

        return result.toArray(new String[0]);
    }

    // 细分法院层级
    private static String[] subdivideCourtLayers(Map<String, String> params) {
        List<String> result = new ArrayList<>(courtLayers.length);
        for (String item : courtLayers) {
            params.put("法院层级", item);
            String data = joinParamMaps(params);
            params.remove("法院层级");
            result.add(data);
        }

        return result.toArray(new String[0]);
    }

    // 细分审判程序
    public static String[] subdivideJudgePrograms(Map<String, String> params) {
        List<String> result = new ArrayList<>(judgePrograms.length);
        for (String item : judgePrograms) {
            params.put("审判程序", item);
            String data = joinParamMaps(params);
            params.remove("审判程序");
            result.add(data);
        }

        return result.toArray(new String[0]);
    }

    // 细分法院地域
    private static String[] subdivideCourtProvinces(Map<String, String> params) {
        List<String> result = new ArrayList<>(courtProvinces.length);
        for (String item : courtProvinces) {
            params.put("法院地域", item);
            String data = joinParamMaps(params);
            params.remove("法院地域");
            result.add(data);
        }

        return result.toArray(new String[0]);
    }

    // 将参数合并为string
    public static String joinParamMaps(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        params.forEach((key, value) -> {
            if (key.contains(" ") || value.contains(" ")) {
                logger.error("params key and value can't contain space. {} - {}", key, value);
            } else {
                sb.append(key);
                sb.append(":");
                sb.append(value);
                sb.append(",");
            }
        });
        // 删掉最后一个,
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    // 将string拆分为参数对
    static Map<String, String> splitParams(String params) {
        Map<String, String> result = new HashMap<>();
        String[] data = params.split(",");
        for (String item : data) {
            String[] pair = item.split(":");
            result.put(pair[0], pair[1]);
        }
        return result;
    }
}
