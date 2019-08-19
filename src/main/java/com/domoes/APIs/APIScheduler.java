package com.domoes.APIs;

import com.domoes.Driver;
import com.domoes.mongodb.MongoUtils;
import com.domoes.utils.TaskCounter;
import com.domoes.utils.UnzippingInterceptor;
import com.google.common.util.concurrent.RateLimiter;
import okhttp3.*;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Proxy;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liufei on 2019/6/22.
 * API接口调度器
 *   获取服务端API所需参数
 */
public class APIScheduler {
    private static final Logger logger = LoggerFactory.getLogger(APIScheduler.class);
    private static WebDriver driver;
    private static boolean useProxy;
    private static String proxyHost;
    private static int proxyPort;
    private static boolean headless;

    private static OkHttpClient okHttpClient;
    private static JavascriptExecutor jsExecutor;
    private static RateLimiter rateLimiter;
    private static int waitMiliSecondsForLimiter;
    private static boolean switchIp;

    private static String cookieStr;
    private static String guidStr;
    private static String vl5xStr;

    // proxy switch记录
    private static String lastProxyIp;
    private static long lastSwitchTime;
    private static TaskCounter proxyIpCounter = new TaskCounter(100000, 30, Integer.MAX_VALUE);

    public final static String cookiePageUrl = "http://wenshu.court.gov.cn/List/List?sorttype=1&conditions=searchWord+1+AJLX++%E6%A1%88%E4%BB%B6%E7%B1%BB%E5%9E%8B:%E5%88%91%E4%BA%8B%E6%A1%88%E4%BB%B6";
    private static Pattern docIdPattern;

    // 主窗口 (tab页, 只在这个tab页里刷新ip和cookie)
    private static String mainWindow;

    public static boolean init(boolean useProxy, boolean useRemoteDriver, boolean headless, String proxyIpAndPort, double rateLimit, boolean switchIp) {
        logger.info("useProxy = {}, useRemoteDriver = {}, headless = {}, proxyIpAndPort = {}", useProxy, useRemoteDriver, headless, proxyIpAndPort);
        APIScheduler.useProxy = useProxy;
        APIScheduler.switchIp = switchIp;
        APIScheduler.headless = headless;
        ChromeOptions options = new ChromeOptions();

        if (useProxy) {
            Proxy proxy = new Proxy();
            proxy.setHttpProxy(proxyIpAndPort).setFtpProxy(proxyIpAndPort).setSslProxy(proxyIpAndPort);
            logger.info("set socket proxy {}", proxyIpAndPort);
            options.setProxy(proxy);

            parseProxyHost(proxyIpAndPort);
        }
        options.setHeadless(headless);

        options.addArguments("start-maximized"); // open Browser in maximized mode
        options.addArguments("disable-infobars"); // disabling infobars
        options.addArguments("--disable-extensions"); // disabling extensions
        options.addArguments("--disable-gpu"); // applicable to windows os only
        options.addArguments("--disable-dev-shm-usage"); // overcome limited resource problems
        options.addArguments("--no-sandbox"); // Bypass OS security model

        if (useRemoteDriver) {
            try {
                driver = new RemoteWebDriver(options);
            } catch (Exception e) {
                logger.error("create remote driver failed. {}", e.getMessage());
                return false;
            }
        } else {
            String driverPath = System.getProperty("webdriver.chrome.driver");
            if (driverPath == null) {
                logger.error("must set webdriver.chrome.driver property in local driver mode.");
                return false;
            } else
                logger.info("webdriver.chrome.driver = {}", driverPath);
            driver = new ChromeDriver(options);
        }

        rateLimiter = RateLimiter.create(rateLimit);
        waitMiliSecondsForLimiter = (int)(1000 / rateLimit);

        docIdPattern = Pattern.compile("\\S{8}-\\S{4}-\\S{4}-\\S{4}-\\S{12}");

        driver.get(cookiePageUrl);
        return refresh("system initialize");
    }

    // 检查是否需要更换ip
    // 这个接口用于定时检查是否要换ip, 如果5分钟内刚刚换过ip, 则不需要再换
    public static void checkRefresh() {
        if (switchIp && System.currentTimeMillis() - lastSwitchTime > 5 * 60 * 1000)
            refresh("period refresh ip");
    }

    // 刷新参数
    // message表示是因为什么原因需要刷新ip
    static boolean refresh(String message) {
        int loopCount = 0;
        int maxLoopCount = 50;      // 最多尝试50次

        // 第一次refresh必须调用, 否则jsExecutor会一直是null
        //if (jsExecutor != null && !switchIp)
        //    return true;

        if (switchIp)
            switchIp(message);

        do {
            // 先删除cookie后重新获取
            try {
                if (driver != null)
                    driver.manage().deleteAllCookies();
            } catch (Exception e) {
                logger.warn("exception at delete cookies. {}", e.getMessage());
                // 如果异常是invalid session id, 只能退出重启
                if (e.getMessage().startsWith("invalid session id")) {
                    Driver.stop();
                    Runtime.getRuntime().exit(0);
                }
                break;
            }

            if (!joinCookies()) {
                logger.error("get cookie failed.");
                continue;
            }

            if (driver instanceof JavascriptExecutor) {
                jsExecutor = (JavascriptExecutor) driver;
            } else {
                logger.error("driver is not a JavascriptExecutor");
                continue;
            }

            // 生成guid
            guidStr = String.format("%s%s-%s-%s%s-%s%s%s",
                    randomString(jsExecutor), randomString(jsExecutor),
                    randomString(jsExecutor),
                    randomString(jsExecutor), randomString(jsExecutor),
                    randomString(jsExecutor), randomString(jsExecutor), randomString(jsExecutor));
            logger.info("guid is {}", guidStr);
            guidStr = encodeValue(guidStr);

            // 计算vl5x
            vl5xStr = callGetKey(jsExecutor);
            if (vl5xStr == null) {
                logger.error("failed to get key.");
                continue;
            }
            vl5xStr = encodeValue(vl5xStr);

            if (okHttpClient == null) {
                if (useProxy) {
                    logger.info("okhttp by proxy");
                    java.net.Proxy proxy = new java.net.Proxy(java.net.Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
                    okHttpClient = new OkHttpClient.Builder()
                            .connectTimeout(30, TimeUnit.SECONDS)
                            .readTimeout(30, TimeUnit.SECONDS)
                            .proxy(proxy)
                            .connectionPool(new ConnectionPool(5, 1, TimeUnit.MINUTES))
                            .addInterceptor(new UnzippingInterceptor())
                            .build();
                } else {
                    okHttpClient = new OkHttpClient.Builder()
                            .connectTimeout(30, TimeUnit.SECONDS)
                            .readTimeout(30, TimeUnit.SECONDS)
                            .connectionPool(new ConnectionPool(5, 1, TimeUnit.MINUTES))
                            .addInterceptor(new UnzippingInterceptor())
                            .build();
                }
            }

            break;
        } while (++loopCount < maxLoopCount);

        return vl5xStr != null;
    }

    // 获取driver对象后会用于发http请求, rateLimit使用数量加1
    public static WebDriver getDriver() {
        rateLimiter.tryAcquire(waitMiliSecondsForLimiter, TimeUnit.MILLISECONDS);
        return driver;
    }

    // 获取okhttp对象后会用于发http请求, rateLimit使用数量也加1
    static OkHttpClient getOkHttpClient() {
        rateLimiter.tryAcquire(waitMiliSecondsForLimiter, TimeUnit.MILLISECONDS);
        return okHttpClient;
    }

    // 限流计数加1, 超时时间为1s, 超过1s后会恢复
    public static void acquireRateLimit() {
        rateLimiter.tryAcquire(waitMiliSecondsForLimiter, TimeUnit.MILLISECONDS);
    }

    public static JavascriptExecutor getJsExecutor() {
        return jsExecutor;
    }

    public static String encodeValue(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException ex) {
            logger.warn("encode value failed. {}", ex.getMessage());
            return null;
        }
    }

    static String decodeValue(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException ex) {
            logger.warn("decode value failed. {}", ex.getMessage());
            return null;
        }
    }

    // 检查docId是否合法
    public static boolean isValidDocId(String docId) {
        Matcher m = docIdPattern.matcher(docId);
        return m.find();
    }

    private static String randomString(JavascriptExecutor jsExecutor) {
        try {
            Object result = jsExecutor.executeScript("return createGuid();");
            logger.info("createGuid() result: {}", result.toString());
            if (result instanceof String)
                return (String) result;
        } catch (Exception e) {
            logger.debug("exception at jsExecutor.executeScript createGuid(). {}", e.toString());
        }

        logger.error("generate random guid string failed.");
        return "d3s9";
    }

    private static String callGetKey(JavascriptExecutor jsExecutor) {
        try {
            Object result = jsExecutor.executeScript("return getKey();");
            logger.info("getKey() result: {}", result.toString());
            if (result instanceof String)
                return (String) result;
        } catch (Exception e) {
            logger.debug("exception at jsExecutor.executeScript getKey(). {}", e.toString());
        }

        logger.error("getKey() failed.");
        return null;
    }

    static String getGUID() {
        return guidStr;
    }

    static String getVl5x() {
        return vl5xStr;
    }

    static String getCookie() {
        return cookieStr;
    }

    static boolean isProxyMode() {
        return useProxy;
    }

    static boolean isHeadless() {
        return headless;
    }

    private static void parseProxyHost(String ipAndPort) {
        String[] items = ipAndPort.split(":");
        if (items.length != 2) {
            logger.warn("invalid ipAndPort {}", ipAndPort);
            return;
        }

        proxyHost = items[0];
        try {
            proxyPort = Integer.parseInt(items[1]);
        } catch (Exception e) {
            logger.warn("exception at parse ipAndPort {}, {}", ipAndPort, e.getMessage());
        }
    }

    // 切换ip
    private static void switchIp(String reason) {
        logger.info("useProxy={}", useProxy);
        if (useProxy) {
            String url = driver.getCurrentUrl();
            logger.info("switch ip, current url: {}", url);
            driver.get("http://proxy.abuyun.com/switch-ip");
            String result = driver.getPageSource();
            logger.info("switch ip result: {}. start load main page for cookie...", result);

            try {
                result = result.replaceAll("</?.+?/?>", "");
                logger.info("switch ip result: {}", result);
                String[] items = result.split(",");
                if (items.length < 3)
                    logger.warn("invalid switch ip result. {}", result);
                else {
                    // 记录切换ip的行为
                    String oldIp = lastProxyIp;
                    lastProxyIp = items[0];
                    lastSwitchTime = System.currentTimeMillis();
                    MongoUtils.insertSwitchIpRecord(lastProxyIp, result, oldIp, reason, getProcessName());

                    checkIpUsability();
                }
            } catch (Exception e) {
                logger.warn("exception at switch ip. {}", e.getMessage());
            }

            // 重新回到原来的url
            acquireRateLimit();
            driver.navigate().to(url);
        }
    }

    // 检查IP的可用性 (只是检查, 因为abuyun有限制, 并不能立即切换ip)
    private static void checkIpUsability() {
        // 当前检查条件: 半小时内重复出现的就报警
        proxyIpCounter.incCount(lastProxyIp);
        int count = proxyIpCounter.getCount(lastProxyIp);
        if (count > 2)
            logger.warn("ip {} returned {} times in 30 minutes.", lastProxyIp, count);
    }

    private static boolean joinCookies() {
        // 先保留当前tab, 在定时刷新ip的时候有可能打开了多个tab, 需要回到主tab页去刷新, 然后再回到原来的tab页
        String currentWindow = driver.getWindowHandle();
        if (mainWindow == null)
            mainWindow = currentWindow;

        // 获取cookie (必须在列表页获取完整cookie, 在首页只能拿到有限的几条cookie)
        logger.info("start load main page for cookie...");
        acquireRateLimit();
        String url;
        try {
            if (!currentWindow.equals(mainWindow))
                driver.switchTo().window(mainWindow);
            url = driver.getCurrentUrl();
            driver.get(cookiePageUrl);
        } catch (Exception e) {
            logger.warn("exception at get cookie url. {}", e.getMessage());
            return false;
        }

        logger.info("cookie info:");
        try {
            List<String> cookies = new ArrayList<>();
            Set<Cookie> allCookies = driver.manage().getCookies();
            for (Cookie loadedCookie : allCookies) {
                logger.info("{} -> {}", loadedCookie.getName(), loadedCookie.getValue());
                cookies.add(loadedCookie.getName() + "=" + loadedCookie.getValue());
            }
            cookieStr = String.join("; ", cookies);
            logger.info("cookie string: {}", cookieStr);
        } catch (Exception e) {
            logger.warn("exception. {}", e.getMessage());
            return false;
        } finally {
            // 返回原来的tab页
            driver.navigate().to(url);
            if (!currentWindow.equals(mainWindow))
                driver.switchTo().window(currentWindow);
        }

        return true;
    }

    // 获取进程名
    private static String getProcessName() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        if (runtimeMXBean != null)
            return runtimeMXBean.getName();
        return "";
    }

    public static void stop() {
        if (driver != null)
            driver.quit();
    }
}
