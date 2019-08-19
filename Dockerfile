# Chrome安装目录: /usr/bin/chrome-stable (version: 75.0.3770.100-1)
# ChromeDriver安装目录: /usr/bin/chromedriver
# openjdk-8-jre
# Selenium 3.14.0 (不需要)

FROM openjdk:8-jre
MAINTAINER helloqinglan <helloqinglan@gmail.com>

# install chrome
COPY chrome/google-chrome-stable_current_amd64.deb /tmp/
COPY chromedriver/75/linux/chromedriver /usr/bin/chromedriver
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libappindicator1 \
        libappindicator3-1 \
        fonts-liberation \
        libasound2 \
        libatk-bridge2.0-0 \
        libatspi2.0-0 \
        libgtk-3-0 \
        libnspr4 \
        libnss3 \
        libx11-xcb1 \
        libxss1 \
        libxtst6 \
        lsb-release \
        xdg-utils && \
    dpkg -i /tmp/google-chrome-stable_current_amd64.deb && \
    rm -rf /tmp/* && \
    rm -rf /var/cache/apk/* && \
    rm -rf /var/lib/apt/lists/*

# Add Maven dependencies (not shaded into the artifact; Docker-cached)
WORKDIR /usr/share/courtcrawler
COPY target/lib/* lib/

# Add the service itself
ARG JAR_FILE=courtdriver-1.0.0.jar
COPY target/${JAR_FILE} courtcrawler.jar


ENTRYPOINT ["java", "-cp", "courtcrawler.jar:./lib/*", "-Dwebdriver.chrome.driver=/usr/bin/chromedriver", "-XX:+UseG1GC"]

#CMD ["-DswitchIp=true", "-Dheadless=false"]
CMD ["-DacmEndpoint=acm.aliyun.com", "-DacmNamespace=REPLACEME", "-DacmAccessKey=REPLACEME", "-DacmSecretKey=REPLACEME", \
         "-Xms256m", "-Xmx256m", "com.domoes.Driver"]