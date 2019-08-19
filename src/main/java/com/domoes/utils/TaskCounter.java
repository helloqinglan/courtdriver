package com.domoes.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by liufei on 2019/7/4.
 * 任务计数器, 用于检查任务执行了多少次
 */
public class TaskCounter {
    private static Logger logger = LoggerFactory.getLogger(TaskCounter.class);

    // 缓存每个任务的执行次数, 每次失败重新放回队列时次数加1
    // 如果次数超过限制, 则放弃该任务
    // 这个缓存只是为了避免某些有问题的任务无限次数进行重试, 并不会对结果产生影响, 所以不考虑分布式环境的同步问题
    // 假设每条key有100字节, 10万条缓存数据占用内存10M左右
    private LoadingCache<String, Integer> taskCache;
    private int maxTryCount;

    // limitCount表示缓存的条数
    // expireMinutes表示数据多少分钟后失效
    // maxTryCount表示任务最多重试次数
    public TaskCounter(int limitCount, int expireMinutes, int maxTryCount) {
        CacheLoader<String, Integer> loader = new CacheLoader<String, Integer>() {
            @Override
            public Integer load(String s) {
                return null;
            }
        };
        if (expireMinutes > 0)
            taskCache = CacheBuilder.newBuilder()
                    .maximumSize(limitCount)
                    .expireAfterAccess(expireMinutes, TimeUnit.MINUTES)
                    .build(loader);
        else
            taskCache = CacheBuilder.newBuilder()
                    .maximumSize(limitCount)
                    .build(loader);

        this.maxTryCount = maxTryCount;
    }

    // 获取任务的当前计数值
    public int getCount(String key) {
        Integer count = taskCache.getIfPresent(key);
        if (count == null)
            return  0;
        return count;
    }

    // 任务计数加1
    // 返回:
    //    true 表示任务次数在限制内
    //    false 表示任务次数已超过最大限制, 调用方不应该再重试这个任务
    public boolean incCount(String key) {
        Integer count = taskCache.getIfPresent(key);
        if (count == null)
            count = 0;
        logger.info("task {} retry count {}, limit {}", key, count, maxTryCount);

        if (count < maxTryCount) {
            taskCache.put(key, count + 1);
            return true;
        }

        return false;
    }

    // 删除任务记录
    public void remove(String key) {
        taskCache.invalidate(key);
    }
}
