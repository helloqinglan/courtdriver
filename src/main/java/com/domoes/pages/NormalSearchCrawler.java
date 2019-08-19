package com.domoes.pages;

import com.domoes.APIs.APIScheduler;
import com.domoes.kafka.TaskMQ;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liufei on 2019/6/17.
 * 普通搜索任务
 */
public class NormalSearchCrawler {
    private static final Logger logger = LoggerFactory.getLogger(NormalSearchCrawler.class);

    private static final int waitingForSearchTools = 3;
    private static final int sleepingForNextPage = 50;

    public static void search(TaskMQ mq, String keyword) {
        WebDriver driver = APIScheduler.getDriver();
        try {
            // 在列表页做搜索操作
            driver.navigate().to(APIScheduler.cookiePageUrl);
            String lastUrl = driver.getCurrentUrl();

            // 输入搜索关键字
            // 分三步: 点击输入框 (点击后DOM会刷新)
            WebDriverWait wait = new WebDriverWait(driver, waitingForSearchTools);
            WebElement searchInput = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("gover_search_key")));
            searchInput.click();

            // 输入搜索关键字
            searchInput = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("gover_search_key")));
            searchInput.sendKeys(keyword);
            logger.info("search keyword input: {}", keyword);

            // 回车
            searchInput = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("gover_search_key")));
            searchInput.sendKeys(Keys.RETURN);
            logger.info("clicked search button.");

            // 点击搜索后会跳转到list页面, 等待url跳转
            // 页面跳转很快, 但是搜索页的内容加载会比较慢, 所以不断检查url有没有变化
            int loopCount = 0;
            do {
                String url = driver.getCurrentUrl();
                if (!url.equalsIgnoreCase(lastUrl))
                    break;

                try {
                    Thread.sleep(sleepingForNextPage);
                } catch (Exception e) {
                    logger.warn("exception at waiting for new page. {}", e.getMessage());
                }
            } while (loopCount++ < 1000);
        } catch (Exception e) {
            logger.warn("exception at get search element. {}", e.getMessage());
            return;
        }

        ListPageCrawler.load(mq);
    }
}
