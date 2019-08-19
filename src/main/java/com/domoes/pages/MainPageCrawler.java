package com.domoes.pages;

import com.domoes.Driver;
import com.domoes.kafka.TaskMQ;
import org.openqa.selenium.By;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by liufei on 2019/6/1.
 * 抓取首页
 */
public class MainPageCrawler {
    private static final String MAINPAGE_URL = "http://wenshu.court.gov.cn";

    private static final Logger logger = LoggerFactory.getLogger(MainPageCrawler.class);
    private static TaskMQ mqEngine = null;

    public static void parsePage(WebDriver driver, TaskMQ mq) {
        logger.info("navigate to page {}", MAINPAGE_URL);
        mqEngine = mq;
        driver.navigate().to(MAINPAGE_URL);

        // 统计信息 (文书总量)

        getMapContent(driver);
    }

    // 抓取地图信息
    private static void getMapContent(WebDriver driver) {
        WebElement mapDiv;
        try {
            WebDriverWait wait = new WebDriverWait(driver, Driver.waitSecondsForElement);
            mapDiv = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("idx_map_content")));
        } catch (Exception e) {
            logger.warn("exception at get map div. {}", e.getMessage());
            return;
        }

        List<WebElement> childDivs = mapDiv.findElements(By.tagName("div"));
        if (childDivs == null || childDivs.isEmpty()) {
            logger.warn("can't find province list");
            return;
        }

        // 因为点击div后DOM会刷新, 所以这里不能缓存WebElements列表
        // 改为缓存省份名列表, 在循环中用名称去查找Element
        List<String> provinceNames = new ArrayList<>();
        for (WebElement element: childDivs) {
            String province = element.getAttribute("_name");
            logger.info("get province name {}", province);
            provinceNames.add(province);
        }

        // 将列表随机打乱顺序, 避免每次用同样的顺序导致有些省份始终取不到
        Collections.shuffle(provinceNames);

        for (String province : provinceNames) {
            logger.info("searching WebElement for province {}", province);
            clickProvince(driver, province);
        }
    }

    // 点击省份按钮刷新法院列表
    private static void clickProvince(WebDriver driver, String province) {
        WebElement mapDiv;
        // 获取click前的element
        try {
            mapDiv = driver.findElement(By.id("idx_map_content"));
        } catch (Exception e) {
            logger.error("should not caught exception. {}", e.getMessage());
            return;
        }

        List<WebElement> childDivs = mapDiv.findElements(By.tagName("div"));
        if (childDivs == null || childDivs.isEmpty()) {
            logger.error("can't find province list in loop, for searching province {}", province);
            return;
        }

        for (WebElement element: childDivs) {
            String name = element.getAttribute("_name");
            if (name.equals(province)) {
                logger.info("get WebElement for province {}", province);

                // 点击省份, 会显示该省的法院列表
                Actions builder = new Actions(driver);
                builder.moveToElement(element).perform();
                builder = new Actions(driver);
                builder.click(element).perform();

                // 注意
                // 由于点击按钮后DOM会刷新, 但是页面元素并没有显示和隐藏的变化, 无法通过wait.presenceOfElementLocated来判断是否加载完成
                // 这里采用的方法是: 判断一个element是否出现 stale element reference 异常, 出现该异常表示页面已刷新 (但可能未刷新完成)
                // 有些省份点击后就是不会刷新页面 (比如香港、澳门、台湾)
                int loop = 0;
                int maxLoop = 200;
                do {
                    try {
                        mapDiv.findElement(By.tagName("div"));
                        if (loop % 100 == 0)
                            logger.info("page has not reloaded. loop = {}", loop);
                    } catch (StaleElementReferenceException e) {
                        logger.info("get stale element reference exception, page already reloaded.");
                        break;
                    }
                } while (loop ++ < maxLoop);

                getProvinceCourts(driver, province);
                break;
            }
        }
    }

    // 获取该省的法院列表
    // needRetry表示如果未获取到法院列表, 是否重试点击省份按钮
    private static void getProvinceCourts(WebDriver driver, String province) {
        // 有些省份没有法院 (比如香港、澳门、台湾)
        WebElement area;
        try {
            WebDriverWait wait = new WebDriverWait(driver, Driver.waitSecondsForElement);
            area = wait.until(ExpectedConditions.presenceOfElementLocated(By.className("area")));
            area = area.findElement(By.tagName("h2"));  // 新疆比较特殊, 未做处理
        } catch (Exception e) {
            logger.error("can't find province area tag. {}", e.getMessage());
            return;
        }
        String innerName = area.getAttribute("innerHTML");
        if (!innerName.equals(province)) {
            // 当前省份列表与要获取的省份不一致
            // 网页有时候有bug，需要多次点击 (有时候多次点击也没用)
            logger.info("province {} does not have courts, current area is {}", province, innerName);
            return;
        }

        List<WebElement> lis;
        try {
            WebDriverWait wait = new WebDriverWait(driver, Driver.waitSecondsForElement);
            WebElement ul = wait.until(ExpectedConditions.presenceOfElementLocated(By.className("region")));
            ul = ul.findElement(By.tagName("ul"));
            logger.info("get ul element for province {}", province);
            lis = ul.findElements(By.tagName("li"));
            logger.info("get li elements for province {}", province);
        } catch (Exception e) {
            logger.warn("exception at find province {} courts. {}", province, e.getMessage());
            return;
        }

        if (lis == null || lis.isEmpty()) {
            logger.error("can't find child court list tags for province {}", province);
            return;
        }

        // 因为DOM会刷新, 不能缓存WebElement列表
        // 改为先保存名称列表, 然后在循环中用名称去查找Element
        List<String> courtNames = new ArrayList<>();
        for (WebElement el : lis) {
            try {
                el = el.findElement(By.tagName("div"));
                el = el.findElement(By.tagName("a"));
                String name = el.getAttribute("innerHTML");
                logger.info("find court {} in province {}", name, province);

                if (name != null)
                    courtNames.add(name);
            } catch (Exception e) {
                // 这里是正常的, 因为列表中有些不是法院名
                //logger.info("get court name failed for province {}. {}", province, e.getMessage());
            }
        }

        logger.info("court count = {} for province {}", courtNames.size(), province);
        for (String courtName : courtNames) {
            try {
                WebDriverWait wait = new WebDriverWait(driver, Driver.waitSecondsForElement);
                WebElement ul = wait.until(ExpectedConditions.presenceOfElementLocated(By.className("region")));
                ul = ul.findElement(By.tagName("ul"));

                lis = ul.findElements(By.tagName("li"));
                if (lis == null || lis.isEmpty()) {
                    logger.error("searching child court list for province {} failed, court {}", province, courtName);
                    continue;
                }
            } catch (Exception e) {
                logger.warn("exception at find province courts for {}. {}", courtName, e.getMessage());
                continue;
            }

            WebElement liElement = null;
            WebElement hrefElement = null;
            for (WebElement el : lis) {
                try {
                    WebElement divElement = el.findElement(By.tagName("div"));
                    hrefElement = divElement.findElement(By.tagName("a"));
                    String name = hrefElement.getAttribute("innerHTML");
                    if (name.equals(courtName)) {
                        logger.debug("find court {} WebElement in province {}", name, province);
                        liElement = el;
                        break;
                    }
                } catch (Exception e) {
                    // 这里是正常的, 因为列表中有分隔线
                    //logger.info("get court name failed for province {}, court {}. {}", province, courtName, e.getMessage());
                }
            }

            if (liElement == null) {
                logger.warn("has not find court WebElement for {} in province {}", courtName, province);
                continue;
            }

            String key;
            try {
                key = liElement.getAttribute("key");
                String link = hrefElement.getAttribute("href");

                // 有些li没有key, 是正常的, 比如最高人民法院
                logger.info("key = {}, name = {}, link = {}", key, courtName, link);

                // 数据入库
                if (link != null) {
                    mqEngine.pushMessage(link.getBytes(), TaskMQ.TAG_LIST);
                }
            } catch (Exception e) {
                logger.warn("exception at find href element with province {}. {}", province, e.getMessage());
                continue;
            }

            // 鼠标移到地名上, 会显示该法院的基层法院列表
            try {
                logger.info("move to court {} for sub courts.", courtName);
                Actions builder = new Actions(driver);
                builder.moveToElement(liElement).perform();

                courtLinks(driver, province, key, courtName);
            } catch (Exception e) {
                logger.warn("exception at mouse over province {} court {}. {}", province, courtName, e.getMessage());
            }
        }
    }

    // 获取法院的基层法院列表
    private static void courtLinks(WebDriver driver, String province, String key, String courtName) {
        WebDriverWait wait = new WebDriverWait(driver, Driver.waitSecondsForElement);
        wait.until(ExpectedConditions.presenceOfElementLocated(By.className("divarealihide")));

        List<WebElement> elements = driver.findElements(By.className("divarealihide"));
        if (elements == null || elements.isEmpty()) {
            logger.warn("province {} court {} with key {} does not have sub courts", province, courtName, key);
            return;
        }

        logger.info("sub court for province {}, element count {}", province, elements.size());
        for (WebElement element : elements) {
            WebElement href;
            try {
                href = element.findElement(By.tagName("a"));
            } catch (Exception e) {
                logger.warn("exception at get sub court's link for {}. {}", courtName, e.getMessage());
                continue;
            }

            String name = href.getAttribute("innerHTML");
            String link = href.getAttribute("href");
            logger.info("name={}, href={}", name, link);

            // 数据入库
            mqEngine.pushMessage(link.getBytes(), TaskMQ.TAG_LIST);
        }
    }
}
