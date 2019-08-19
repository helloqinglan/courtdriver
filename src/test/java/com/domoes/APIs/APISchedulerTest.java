package com.domoes.APIs;

import org.junit.Assert;

import static org.junit.Assert.*;

public class APISchedulerTest {

    @org.junit.Test
    public void encodeDecode() {
        String value = "中文";
        String encoded = APIScheduler.encodeValue(value);
        String decoded = APIScheduler.decodeValue(encoded);
        Assert.assertEquals(value, decoded);

        value = "english";
        encoded = APIScheduler.encodeValue(value);
        decoded = APIScheduler.decodeValue(encoded);
        Assert.assertEquals(value, decoded);
    }
}