package com.domoes.utils;

import com.domoes.services.HanLPGrpc;
import com.domoes.services.SegmentRequest;
import com.domoes.services.SegmentResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by liufei on 2019/7/5.
 * HanLP gRPC客户端
 */
public class HanLPClient {
    private static Logger logger = LoggerFactory.getLogger(HanLPClient.class);

    private final ManagedChannel channel;
    private final HanLPGrpc.HanLPBlockingStub blockingStub;

    public HanLPClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext()
        .build());
    }

    private HanLPClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = HanLPGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * 分词
     * @param param 需要分词的句子
     * @param method 使用的分词方法
     * @return 分词结果 [{"word":"1", "nature":"ne", "ne":"LOC"}]
     */
    public SegmentResponse segment(String docid, String param, int method) {
        //logger.info("try to segment with param {} method {}", param, method);
        try {
            if (method == 0) {
                logger.warn("service type can't be null.");
                return null;
            }

            SegmentRequest.Builder builder = SegmentRequest.newBuilder().setValue(param).setMethod(method);
            if (docid != null)
                builder.setDocid(docid);
            SegmentResponse response = blockingStub.segment(builder.build());
            if (!validResponse(response)) {
                logger.warn("response is null. {}", response.toString());
                return null;
            }
            return response;
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed. {}", e.getStatus());
            return null;
        }
    }

    private boolean validResponse(SegmentResponse response) {
        // 如果字段全部为空则表示无效
        return response.getViterbiValue() != null ||
                response.getCrfValue() != null ||
                response.getPerceptronValue() != null ||
                response.getBaiduValue() != null;
    }
}
