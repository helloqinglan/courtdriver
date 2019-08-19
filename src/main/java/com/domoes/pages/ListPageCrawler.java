package com.domoes.pages;

import com.domoes.APIs.APIScheduler;
import com.domoes.APIs.ListParams;
import com.domoes.Driver;
import com.domoes.mongodb.MongoUtils;
import com.domoes.kafka.TaskMQ;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by liufei on 2019/6/10.
 * 抓取列表页
 */
public class ListPageCrawler {
    private static final Logger logger = LoggerFactory.getLogger(ListPageCrawler.class);
    private static final int waitingForResultList = 30;     // 秒

    private static List<String> itemIdList = new ArrayList<>();
    private static Map<String, Map<String, String>> itemDescs = new HashMap<>();

    /**
     * 因为从搜索页跳转到列表页时, url是自己跳转的, 不是通过参数输入
     * 所以这里统一为已经完成navigate.to()
     */
    public static boolean load(TaskMQ mq) {
        WebDriver driver = APIScheduler.getDriver();
        String url = driver.getCurrentUrl();
        logger.info("parse list page {}", url);

        // 访问限制, 需要输入验证码
        if (url.contains("VisitRemind")) {
            logger.warn("visit has been forbidden. url = {}", url);
            return true;
        }

        // 过滤掉cookie页 (在多tab页的情况下存在问题, 导致又打开cookie页去抓取)
        if (url.equalsIgnoreCase(APIScheduler.cookiePageUrl))
            return true;

        // 临时修复
        // 部分url条件忘了加conditions=
        if (url.contains("&searchWord")) {
            logger.info("wrong url with &searchWord. {}", url);
            url = url.replaceAll("&searchWord", "&conditions=searchWord");
            logger.info("after replacement is: {}", url);
            driver.get(url);
        }

        String listWindow = driver.getWindowHandle();

        //
        // 列表页的抓取方法
        // 1. 将该列表页上的所有文书点开
        // 2. 翻页并点开所有文书 (因为翻页是AJAX请求, url不会变化, 不能生成新的列表任务, 只能一次将文书任务加载完)
        //    共20页, 200条文书
        // 3. 如果该列表页的文档数量超过200, 尝试细化搜索项
        //
        // 因为翻页后失败再重试又是从第一页开始, 大量的重复检查, 所以对于翻页失败的不再重试 (返回true)
        // 只有在第一页出现错误的才重试 (return currentPage > 1)
        //

        int currentPage = 1;
        do {
            itemIdList.clear();

            // 右侧列表区域
            // body > div main > div contentMiddle > div contentMain > div content > div list > div resultList
            WebElement resultListDiv;
            try {
                // 列表页加载有时候比较慢, 超时时间设长一点
                WebDriverWait wait = new WebDriverWait(driver, waitingForResultList);
                resultListDiv = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("resultList")));
            } catch (Exception e) {
                logger.warn("exception at get resultList div. {}", e.getMessage());
                return currentPage > 1;
            }

            // 等待列表内容加载出来
            List<WebElement> dataItems = null;
            int loopCount = 0;
            do {
                try {
                    dataItems = resultListDiv.findElements(By.className("dataItem"));
                } catch (Exception e) {
                    if (e.getMessage().startsWith("stale element reference: element is not attached to the page document")) {
                        logger.warn("exception at get dataItem element. js reloaded.");
                        return false;
                    }
                }
                if (dataItems == null || dataItems.isEmpty()) {
                    try {
                        String text = resultListDiv.getAttribute("innerHTML");
                        if (text.equalsIgnoreCase("系统繁忙，请您稍后再试。")) {
                            // 系统繁忙, 不能刷新, 也不需要再等待, 直接翻页
                            logger.info("system busy, try next page.");
                            break;
                        } else if (text.equalsIgnoreCase("无符合条件的数据...")) {
                            // 无符合条件的数据
                            logger.info("no result for this search.");
                            return true;
                        }
                    } catch (Exception e) {
                        logger.warn("exception at check system busy.");
                    }

                    if (loopCount % 50 == 0)
                        logger.info("still waiting for get dataItems of url {} failed.", url);
                    try { Thread.sleep(100); } catch (Exception e) { logger.warn("exception {}", e.getMessage()); }
                } else
                    break;
            } while (loopCount++ < 300);

            if (dataItems == null) {
                logger.warn("get dataItems for url {} failed.", url);
            }

            // 在第一页时检查文档数量如果超过200, 尝试细化搜索项
            if (currentPage == 1) {
                WebElement dataCountItem;
                try {
                    dataCountItem = driver.findElement(By.id("span_datacount"));
                } catch (Exception e) {
                    logger.info("can't find data count element. retry this task later.");
                    return false;
                }

                try {
                    String dataCountStr = dataCountItem.getAttribute("innerHTML");
                    logger.info("data count is {}", dataCountStr);
                    if (dataCountStr != null && Integer.parseInt(dataCountStr) > 200) {
                        List<List<String>> result = splitListPages(url);

                        if (result != null) {
                            for (List<String> item : result) {
                                String param = combineConditions(item);
                                String splitUrl = "http://wenshu.court.gov.cn/list/list/?sorttype=1&" + param;
                                if (mq != null)
                                    mq.pushMessage(splitUrl.getBytes(), TaskMQ.TAG_LIST);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("exception at try split pages. {}", e.getMessage());
                    return false;
                }
            }

            // 当前是第几页
            try {
                WebDriverWait wait = new WebDriverWait(driver, Driver.waitSecondsForElement);
                WebElement docBottom = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("docbottom")));

                List<WebElement> current = docBottom.findElements(By.className("current"));
                if (current == null || current.size() < 1)
                    logger.warn("get current page failed.");
                else {
                    String page = current.get(current.size() - 1).getAttribute("innerHTML");
                    logger.info("current page is {}, currentPage = {}", page, currentPage);
                }
            } catch (Exception e) {
                logger.warn("get current page failed. {}", e.getMessage());
            }

            if (dataItems != null) {
                for (WebElement element : dataItems) {
                    getItemDesc(element);
                }
            }

            // 遍历每个item
            // 先检查该文书是否已获取, 再点击打开 (点击后会跳到新打开的tab, 需要切回来)
            logger.info("itemIdList count {}", itemIdList.size());
            for (String id : itemIdList) {
                WebElement dataItem = getElementForItemId(driver, id);
                if (dataItem == null)
                    logger.warn("can't find dataItem {}", id);
                else {
                    String caseNumber = dataItem.getAttribute("casenumber");
                    String title = dataItem.getAttribute("title");
                    if (MongoUtils.wenshuGotFinished(caseNumber, title))
                        logger.info("case {} already has detail content.", caseNumber);
                    else {
                        WebElement href = getLinkElementForItemId(dataItem);
                        if (href != null) {
                            logger.info("try to open link for casenumber {}", caseNumber);
                            try {
                                APIScheduler.acquireRateLimit();

                                Actions builder = new Actions(driver);
                                builder.click(href).perform();

                                // 稍等一点时间试试, 在单搜索案号的列表页时, 点击了链接后跳不回列表页
                                // 频率控制未生效, 这里先加更长的等待时间
                                try { Thread.sleep(200); } catch (Exception e) { logger.warn("exception {}", e.getMessage()); }

                                logger.debug("when ckicked link, current window handles {}", driver.getWindowHandles().size());
                                driver.switchTo().window(listWindow);
                                logger.debug("switch to list window, handles {}, original {}", driver.getWindowHandle(), listWindow);
                            } catch (Exception e) {
                                logger.warn("exception at click doc link {}", href.toString());
                            }
                        } else
                            logger.warn("can't find link for dataItem {}, id {}", caseNumber, id);
                    }
                }
            }

            // 翻页
            boolean nextPageSucceed = nextPage(driver, url);

            // 稍等一点时间, 有时候这里获取windowsHandles时会出问题, 已经打开了多个tab页, 但是返回的还是只有一个
            try { Thread.sleep(50); } catch (Exception e) { logger.warn("exception {}", e.getMessage()); }

            // 依次检查打开的文书页, 如果遇到失败则刷新 (500错误不要刷新, 因为url不正确)
            // 最多刷新3次
            Set<String> windowHandlers = driver.getWindowHandles();
            Set<String> finishedHandlers = new HashSet<>();
            logger.info("current opened window count {}", windowHandlers.size());

            if (!driver.getWindowHandle().equals(listWindow))
                driver.switchTo().window(listWindow);

            if (windowHandlers.size() > 1) {
                for (String window : windowHandlers) {
                    logger.info("check chrome tab {}", window);
                    if (window.equalsIgnoreCase(listWindow) || finishedHandlers.contains(window))
                        continue;

                    driver.switchTo().window(window);

                    // 因为在打开每个文档页的时候已经做了限流计数, 所以这里在分析网页内容的时候不应该再计数, 直接传driver参数
                    // getDriver()方法内部会再进行计数
                    if (!ContentPageCrawler.load(mq, driver, itemDescs)) {
                        logger.info("get doc content failed, refresh tab {}", window);
                        // 刷新网页需要计数
                        APIScheduler.acquireRateLimit();
                        driver.navigate().refresh();
                    } else
                        finishedHandlers.add(window);
                }

                // 关闭除列表页之外的其他tab
                for (String window : windowHandlers) {
                    if (window.equalsIgnoreCase(listWindow))
                        continue;

                    logger.info("close tab {}", window);
                    driver.switchTo().window(window);
                    driver.close();
                }
                driver.switchTo().window(listWindow);
            }

            // 最后一页
            if (isLastPage(driver))
                return true;

            // 翻页失败则可以退出
            if (!nextPageSucceed) {
                logger.warn("go to next page failed, quit page list {}", url);
                return currentPage > 1;
            }
        } while (++currentPage <= Driver.maxPageForList);

        if (!driver.getWindowHandle().equals(listWindow))
            driver.switchTo().window(listWindow);

        return true;
    }

    // 获取文书的概要信息并入库
    private static void getItemDesc(WebElement dataItem) {
        try {
            String id = dataItem.getAttribute("id");
            String title = dataItem.getAttribute("title");                  // 案件标题
            String casecourt = dataItem.getAttribute("casecourt");          // 所属法院
            String casenumber = dataItem.getAttribute("casenumber");        // 案件编号 (可作主键)
            String judgedate = dataItem.getAttribute("judgedate");          // 审判时间
            String ajlx;                                                       // 案件类型 (可有多个, 用,分隔)

            StringBuilder sb = new StringBuilder();
            boolean addComma = false;
            try {
                List<WebElement> labelElements = dataItem.findElements(By.className("ajlx_lable"));
                if (labelElements != null) {
                    for (WebElement label : labelElements) {
                        String data = label.getAttribute("innerHTML");
                        if (addComma)
                            sb.append(", ");
                        else
                            addComma = true;
                        sb.append(data);
                    }
                }
            } catch (Exception e) {
                logger.warn("can't find ajlx_lable for element {}", casenumber);
            }
            ajlx = sb.toString();

            // 检查是否有关联文书
            String glws = "";
            try {
                WebElement glwsElement = dataItem.findElement(By.className("list-glws"));
                WebElement glwsAElement = glwsElement.findElement(By.tagName("a"));
                glws = glwsAElement.getAttribute("innerHTML");
            } catch (Exception e) {
                logger.debug("wenshu does not have glws.");
            }

            logger.info("find wenshu, id {}, casenumber {}", id, casenumber);
            itemIdList.add(id);

            Map<String, String> itemInfo = new HashMap<>();
            itemInfo.put("title", title);
            itemInfo.put("casecourt", casecourt);
            itemInfo.put("judgedate", judgedate);
            itemInfo.put("ajlx", ajlx);
            itemInfo.put("glws", glws);
            itemDescs.put(casenumber, itemInfo);
        } catch (Exception e) {
            logger.warn("exception at get item desc. {}", e.getMessage());
        }
    }

    private static WebElement getElementForItemId(WebDriver driver, String id) {
        try {
            return driver.findElement(By.id(id));
        } catch (Exception e) {
            logger.warn("exception at getElementForItemId {}", id);
            return null;
        }
    }

    private static WebElement getLinkElementForItemId(WebElement dataItem) {
        try {
            WebElement wsTitle = dataItem.findElement(By.className("wstitle"));
            return wsTitle.findElement(By.tagName("a"));
        } catch (Exception e) {
            logger.warn("get link element for wenshu {} failed.", dataItem.getText());
            return null;
        }
    }

    // 翻页到下一页
    private static boolean nextPage(WebDriver driver, String url) {
        WebElement docBottom;
        try {
            WebDriverWait wait = new WebDriverWait(driver, Driver.waitSecondsForElement);
            docBottom = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("docbottom")));
        } catch (Exception e) {
            logger.warn("exception at get docbottom for next page link. {}", e.getMessage());
            return false;
        }

        WebElement next;
        try {
            next = docBottom.findElement(By.className("next"));
        } catch (Exception e) {
            // 没有下一页的时候不记错误日志
            return false;
        }

        try {
            // 下一页不是a标签 (不能点击)
            // 表示已经是最后一页
            if (!next.getTagName().equalsIgnoreCase("a")) {
                logger.info("last page.");
                return false;
            }
            String page = next.getAttribute("innerHTML");

            Actions builder = new Actions(driver);
            builder.click(next).perform();
            logger.info("clicked next page {} for list {}", page, url);

            return true;
        } catch (Exception e) {
            logger.warn("exception at click next page link. {}", e.getMessage());
            return false;
        }
    }

    private static boolean isLastPage(WebDriver driver) {
        WebElement docBottom;
        try {
            WebDriverWait wait = new WebDriverWait(driver, Driver.waitSecondsForElement);
            docBottom = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("docbottom")));
        } catch (Exception e) {
            logger.warn("exception at get docbottom for check isLastPage. {}", e.getMessage());
            return true;
        }

        WebElement next;
        try {
            next = docBottom.findElement(By.className("next"));
        } catch (Exception e) {
            // 没有下一页的时候不记错误日志
            return true;
        }

        try {
            // 下一页不是a标签 (不能点击)
            // 表示已经是最后一页
            return !next.getTagName().equalsIgnoreCase("a");
        } catch (Exception e) {
            logger.warn("exception at check isLastPage. {}", e.getMessage());
            return true;
        }
    }

    // 将该列表任务拆分为更细的子任务
    static List<List<String>> splitListPages(String url) {
        int index = url.indexOf("conditions");
        if (index < 0) {
            logger.warn("can't find conditions in url {}", url);
            return null;
        }

        String subUrl = url.substring(index);
        // parse后会将+转为空格
        List<NameValuePair> pairs = URLEncodedUtils.parse(subUrl, StandardCharsets.UTF_8);

        List<String> conditions = new ArrayList<>();
        for (NameValuePair i : pairs) {
            logger.info("{} - {}", i.getName(), i.getValue());
            if (i.getName().equalsIgnoreCase("conditions")) {
                String[] items = i.getValue().split(" ");
                if (items.length != 5) {
                    logger.warn("invalid condition value {}", i.getValue());
                } else
                    conditions.add(items[4]);
            }
        }

        // 如果查询条件只有"案号", 对其做特殊处理, 案号只能再拆分审判程序和文书类型
        if (conditions.size() == 1) {
            String[] items = conditions.get(0).split(":");
            if (items.length == 2 && items[0].equals("案号")) {
                return ListParams.subdivideRequestsForCasenumber(conditions);
            }
        }

        return ListParams.subdivideRequests(conditions);
    }

    public static String combineConditions(List<String> params) {
        List<String> conditions = new ArrayList<>();
        for (String item : params) {
            String[] datas = item.split(":");
            if (datas.length != 2) {
                logger.warn("invalid condition {}", item);
                continue;
            }

            // 全文检索和案件类型需要特殊处理
            String value;
            if (datas[0].equalsIgnoreCase("全文检索")) {
                value = String.format("conditions=searchWord+QWJS+++全文检索:%s", datas[1]);
            } else if (datas[0].equalsIgnoreCase("案件类型")) {
                value = String.format("conditions=searchWord+2+AJLX++案件类型:%s", datas[1]);
            } else {
                value = String.format("conditions=searchWord+%s+++%s:%s", datas[0], datas[0], datas[1]);
            }
            conditions.add(value);
        }

        return String.join("&", conditions);
    }
}
