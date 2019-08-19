package com.domoes.APIs;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ListParamsTest {

    @Test
    public void joinParamMaps() {
        Map<String, String> params = new HashMap<>();
        params.put("a", "1");
        params.put("b", "2");
        String result = ListParams.joinParamMaps(params);
        Assert.assertEquals("a:1,b:2", result);

        // 忽略带空格的参数
        params.clear();
        params.put("a", "1 2");
        params.put("b c", "2");
        params.put("c", "3");
        result = ListParams.joinParamMaps(params);
        Assert.assertEquals("c:3", result);
    }

    @Test
    public void splitParams() {
        String params = "a:1,b:2";
        Map<String, String> pair = ListParams.splitParams(params);
        Assert.assertEquals("1", pair.get("a"));
        Assert.assertEquals("2", pair.get("b"));
    }

    @Test
    public void subdivideCaseTypes() {
        Map<String, String> params = new HashMap<>();
        params.put("a", "1");
        params.put("b", "2");
        String[] result = ListParams.subdivideCaseTypes(params);
        Assert.assertEquals(ListParams.caseTypes.length, result.length);
        String data = result[0];
        Map<String, String> mapData = ListParams.splitParams(data);
        Assert.assertEquals(3, mapData.size());
        Assert.assertTrue(mapData.containsKey("案件类型"));
        Assert.assertEquals("1", mapData.get("a"));
        Assert.assertEquals("2", mapData.get("b"));
    }
}