package com.domoes.pages;

import com.domoes.APIs.ListParams;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ListPageCrawlerTest {

    @Test
    public void splitListPages() {
        // 多个查询条件, 会按文书类型进行拆分
        String url = "http://wenshu.court.gov.cn/list/list/?conditions=searchWord+%E6%A1%88%E4%BB%B6%E7%B1%BB%E5%9E%8B+++%E6%A1%88%E4%BB%B6%E7%B1%BB%E5%9E%8B:%E5%88%91%E4%BA%8B%E6%A1%88%E4%BB%B6&conditions=searchWord+%E8%A3%81%E5%88%A4%E5%B9%B4%E4%BB%BD+++%E8%A3%81%E5%88%A4%E5%B9%B4%E4%BB%BD:2010&conditions=searchWord+%E5%85%B3%E9%94%AE%E8%AF%8D+++%E5%85%B3%E9%94%AE%E8%AF%8D:%E5%88%A9%E6%81%AF&conditions=searchWord+%E6%B3%95%E9%99%A2%E5%B1%82%E7%BA%A7+++%E6%B3%95%E9%99%A2%E5%B1%82%E7%BA%A7:%E4%B8%AD%E7%BA%A7%E6%B3%95%E9%99%A2";
        List<List<String>> result = ListPageCrawler.splitListPages(url);
        Assert.assertNotNull(result);
        Assert.assertEquals(ListParams.wenshuTypes.length, result.size());
    }

    @Test
    public void splitCasenumberList() {
        // 只有案号一个查询条件, 会按文书类型和审判程序进行拆分
        String url = "http://wenshu.court.gov.cn/list/list/?sorttype=1&conditions=searchWord+%E6%A1%88%E5%8F%B7+++%E6%A1%88%E5%8F%B7:abc";
        List<List<String>> result = ListPageCrawler.splitListPages(url);
        Assert.assertNotNull(result);
        Assert.assertEquals(ListParams.wenshuTypes.length + ListParams.judgePrograms.length, result.size());
    }

    @Test
    public void combineConditions() {
        List<String> params = new ArrayList<>();
        params.add("a:1");
        params.add("b:2");
        String result = ListPageCrawler.combineConditions(params);
        Assert.assertEquals("conditions=searchWord+a+++a:1&conditions=searchWord+b+++b:2", result);
    }
}