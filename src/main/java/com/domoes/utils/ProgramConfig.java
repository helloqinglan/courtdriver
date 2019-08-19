package com.domoes.utils;

import com.alibaba.edas.acm.ConfigService;
import com.alibaba.edas.acm.exception.ConfigException;
import com.alibaba.edas.acm.listener.PropertiesListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by liufei on 2019/7/3.
 * 程序配置
 */
public class ProgramConfig {
    private static Logger logger = LoggerFactory.getLogger(ProgramConfig.class);

    private static Properties driverProps = new Properties();
    private static Properties mongoProps = new Properties();
    private static Properties kafkaProps = new Properties();
    private static Properties hanlpProps = new Properties();

    private static boolean useRemoteDriver;
    private static boolean useProxy;
    private static boolean headless;
    private static double rateLimit;
    private static boolean switchIp;
    private static boolean ignoreListTask;
    private static boolean ignoreApiTask;

    public static boolean parse() {
        String acmAccessKey;
        String acmSecretKey;
        String acmEndpoint;
        String acmNamespace;
        try {
            acmAccessKey = System.getProperty("acmAccessKey", "please set ACM AccessKey");
            acmSecretKey = System.getProperty("acmSecretKey", "please set ACM SecretKey");
            acmEndpoint = System.getProperty("acmEndpoint", "please set ACM endpoint");
            acmNamespace = System.getProperty("acmNamespace", "please set ACM namespace");
        } catch (Exception e) {
            logger.warn("exception at get ACM params. {}", e.getMessage());
            return false;
        }

        try {
            Properties properties = new Properties();
            properties.put("endpoint", acmEndpoint);
            properties.put("namespace", acmNamespace);

            // Access ACM with instance RAM role: https://help.aliyun.com/document_detail/72013.html
            // properties.put("ramRoleName", "$ramRoleName");

            properties.put("accessKey", acmAccessKey);
            properties.put("secretKey", acmSecretKey);
            ConfigService.init(properties);

            // 加载并监听配置改变
            driverProps = ConfigService.getConfig2Properties("com.domoes.courtcrawls.driver", "courtcrawls", 10000);
            if (!parseDriverConfigs()) {
                logger.warn("parse driver config failed.");
                return false;
            }
            ConfigService.addListener("com.domoes.courtcrawls.driver", "courtcrawls", new PropertiesListener() {
                @Override
                public void innerReceive(Properties properties) {
                    driverProps = properties;
                    logger.info("driver config has changed.");
                    if (!parseDriverConfigs())
                        logger.warn("parse driver config failed.");
                }
            });

            mongoProps = ConfigService.getConfig2Properties("com.domoes.courtcrawls.mongo", "courtcrawls", 10000);
            ConfigService.addListener("com.domoes.courtcrawls.mongo", "courtcrawls", new PropertiesListener() {
                @Override
                public void innerReceive(Properties properties) {
                    mongoProps = properties;
                    logger.info("mongo config has changed.");
                }
            });

            kafkaProps = ConfigService.getConfig2Properties("com.domoes.courtcrawls.kafka", "courtcrawls", 10000);
            ConfigService.addListener("com.domoes.courtcrawls.kafka", "courtcrawls", new PropertiesListener() {
                @Override
                public void innerReceive(Properties properties) {
                    kafkaProps = properties;
                    logger.info("kafka config has changed.");
                }
            });

            hanlpProps = ConfigService.getConfig2Properties("com.domoes.courtcrawls.hanlpservice", "courtcrawls", 10000);
            ConfigService.addListener("com.domoes.courtcrawls.hanlpservice", "courtcrawls", new PropertiesListener() {
                @Override
                public void innerReceive(Properties properties) {
                    hanlpProps = properties;
                    logger.info("hanlpservice config has changed.");
                }
            });
        } catch (ConfigException e) {
            logger.warn("exception at get ACM config. {}", e.getErrMsg());
            return false;
        }

        return true;
    }

    private static boolean parseDriverConfigs() {
        useRemoteDriver = Boolean.parseBoolean(driverProps.getProperty("useRemoteDriver", "false"));
        useProxy = Boolean.parseBoolean(driverProps.getProperty("useProxy", "false"));
        headless = Boolean.parseBoolean(driverProps.getProperty("headless", "false"));
        rateLimit = Double.parseDouble(driverProps.getProperty("rateLimit", "5"));
        switchIp = Boolean.parseBoolean(driverProps.getProperty("switchIp", "false"));

        ignoreListTask = Boolean.parseBoolean(driverProps.getProperty("ignoreListTask", "false"));
        ignoreApiTask = Boolean.parseBoolean(driverProps.getProperty("ignoreApiTask", "false"));

        // 支持通过系统变量修改switchIp参数
        // 因为必须至少要有一个节点配置了switchIp
        try {
            if (System.getProperty("switchIp") != null)
                switchIp = Boolean.parseBoolean(System.getProperty("switchIp"));
            if (System.getProperty("headless") != null)
                headless = Boolean.parseBoolean(System.getProperty("headless"));
        } catch (Exception e) {
            logger.warn("system variable switchIp or headless invalid. {}, {}", System.getProperty("switchIp"), driverProps.getProperty("headless"));
        }

        return true;
    }

    public static boolean isUseRemoteDriver() {
        return useRemoteDriver;
    }

    public static boolean isUseProxy() {
        return useProxy;
    }

    public static boolean isHeadless() {
        return headless;
    }

    public static String getProxyIpAndPort() {
        return driverProps.getProperty("proxyIpAndPort", "10.0.0.18:3128");
    }

    public static double getRateLimit() {
        return rateLimit;
    }

    public static boolean isSwitchIp() {
        return switchIp;
    }

    public static String getMongoUri() {
        return mongoProps.getProperty("mongoUri", "mongodb://10.0.0.18:27017");
    }

    public static String getMongoDb() {
        return mongoProps.getProperty("mongoDb", "courtcrawls");
    }

    public static String getMongoCollection() {
        return mongoProps.getProperty("mongoCollection", "wenshu");
    }

    public static String getKafkaServerAddr() {
        return kafkaProps.getProperty("kafkaServerAddr", "10.0.0.18:9092");
    }

    public static String getKafkaTopic() {
        return kafkaProps.getProperty("kafkaTopic", "courtcrawls");
    }

    public static boolean isIgnoreListTask() {
        return ignoreListTask;
    }

    public static boolean isIgnoreApiTask() {
        return ignoreApiTask;
    }

    public static String getHanlpServiceAddr() {
        return hanlpProps.getProperty("hanlpServiceAddr", "localhost");
    }

    public static int getHanlpServicePort() {
        try {
            return Integer.parseInt(hanlpProps.getProperty("hanlpServicePort", "50051"));
        } catch (Exception e) {
            logger.warn("invalid hanlp service port. {}", e.getMessage());
            return 50051;
        }
    }
}
