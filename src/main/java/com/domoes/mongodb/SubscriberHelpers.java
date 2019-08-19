package com.domoes.mongodb;

import com.mongodb.MongoTimeoutException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by liufei on 2019/6/15.
 * Mongodb辅助类
 */
final class SubscriberHelpers {

    // 提供阻塞等待结果的方法
    public static class ObservableSubscriber<T> implements Subscriber<T> {
        private static Logger logger = LoggerFactory.getLogger(ObservableSubscriber.class);
        private final List<T> received;
        private final List<Throwable> errors;
        private final CountDownLatch latch;
        private volatile Subscription subscription;

        ObservableSubscriber() {
            this.received = new ArrayList<>();
            this.errors = new ArrayList<>();
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onSubscribe(final Subscription s) {
            subscription = s;
        }

        @Override
        public void onNext(final T t) {
            received.add(t);
        }

        @Override
        public void onError(final Throwable t) {
            logger.info("mongodb subscribe result error. {} - {}", t.getMessage(), t.getCause());
            errors.add(t);
            onComplete();
        }

        @Override
        public void onComplete() {
            latch.countDown();
        }

        List<T> getReceived() {
            return received;
        }

        public Throwable getError() {
            if (errors.size() > 0) {
                return errors.get(0);
            }
            return null;
        }

        // 等待并返回结果
        public List<T> get(final long timeout, final TimeUnit unit) throws Throwable {
            return await(timeout, unit).getReceived();
        }

        ObservableSubscriber<T> await(final long timeout, final TimeUnit unit) throws Throwable {
            subscription.request(Integer.MAX_VALUE);
            if (!latch.await(timeout, unit)) {
                throw new MongoTimeoutException("Publisher onComplete timed out");
            }
            if (!errors.isEmpty()) {
                throw errors.get(0);
            }
            return this;
        }
    }

    // 立即执行的订阅器
    public static class OperationSubscriber<T> extends ObservableSubscriber<T> {

        @Override
        public void onSubscribe(final Subscription s) {
            super.onSubscribe(s);
            s.request(Integer.MAX_VALUE);
        }
    }

    // 打印结果的订阅器
    public static class PrintSubscriber<T> extends OperationSubscriber<T> {
        private final Logger logger;
        private final String message;

        PrintSubscriber(final Logger logger, final String message) {
            this.logger = logger;
            this.message = message;
        }

        @Override
        public void onComplete() {
            logger.info(message, getReceived());
            super.onComplete();
        }
    }


    private SubscriberHelpers() {
    }
}
