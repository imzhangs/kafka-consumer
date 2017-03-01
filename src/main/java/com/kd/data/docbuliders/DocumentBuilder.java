package com.kd.data.docbuliders;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.kd.commons.consts.HtmlRegexConsts;
import com.kd.commons.consts.StringFormatConsts;
import com.kd.commons.domain.AbstractDocument;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.enums.ExplainTypeEnum;
import com.kd.commons.http.browser.BrowserFactory;
import com.kd.commons.utils.MD5Util;

@Component
public class DocumentBuilder {

	static Logger logger = LoggerFactory.getLogger(DocumentBuilder.class);




	public static AbstractDocument docBuilder(KafkaMessage message) {
		AbstractDocument doc = null;
		if (message == null) {
			return doc;
		}
		if (message.getUrl() == null) {
			return doc;
		}
		if (message.getBuildDocType() == null) {
			return doc;
		}

		if (StringUtils.isBlank(message.getContent())) {
			String sources = "";
			if (StringUtils.isNotBlank(message.getTempFilePath())) {
				try {
					sources = FileUtils.readFileToString(new File(message.getTempFilePath()), "utf-8");
					message.setContent(sources);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (StringUtils.isBlank(message.getContent())) {
				WebDriver driver = null;
				try {
					driver = BrowserFactory.createWindowsPhantomJS();
					driver.get(message.getUrl());
					WebDriverWait wait = new WebDriverWait(driver, 30);
					wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));
					sources = driver.getPageSource();
				} catch (Throwable e) {
					logger.error(e.getCause().toString());
				} finally {
					message.setContent(sources);
					if (driver != null) {
						driver.close();
						driver.quit();
					}
				}
			}
		}

		if (StringUtils.isNotBlank(message.getContent()) && message.getBuildDocType() != null) {

			switch (message.getBuildDocType()) {
			case newsDoc:
				doc = NewsDocumentBuilder.defaultNewsDocBuild(message.getUrl(), message.getContent());
				break;
			case topicDoc:
				doc = BrowserDocumentBuilder.browserSearchDocBuild(message.getUrl(), message.getContent(), true);
				break;
			case weixinGzhDoc:
				doc = WeixinGzhDocumentBuilder.browserWeixinGzhDocBuild(message.getUrl(), message.getContent(), true);
				break;
			default:
				break;
			}
		} else {
			logger.error("message content or buildDocType is null ....");
		}
		if(null==doc){
			logger.error("doc is null .........");
			return doc;
		}
		
		String tempFilePath = "/data/htmlsource/" + DateFormatUtils.format(new Date(), StringFormatConsts.DATE_NUMBER_FORMAT)+"/";
		tempFilePath = tempFilePath + MD5Util.MD5(message.getUrl());
		try {
			FileUtils.writeStringToFile(new File(tempFilePath), JSONObject.toJSONString(doc), "utf-8", false);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			//分词切割
//			List<Word> wordList = new ArrayList<Word>();
//			if (jedis != null) {
//				wordList = WordSegmenter.seg(value.htmlContent, SegmentationAlgorithm.BidirectionalMaximumMatching);
//				for (Word w : wordList) {
//					jedis.sadd("redis-words", w.getText());
//				}
//			}
		}

		return doc;
	}

	public static String docBuilderString(KafkaMessage message) {
		AbstractDocument doc = docBuilder(message);
		if(doc==null){
			return "";
		}
		return JSONObject.toJSONString(doc);
	}

	@SuppressWarnings("finally")
	public static KafkaMessage buildTempFile(KafkaMessage message) {
		String sources = "";
		WebDriver driver = null;
		try {
			driver = BrowserFactory.createWindowsPhantomJS();
			driver.get(message.getUrl());
			message.setType(ExplainTypeEnum._buildDocument);
			sources = driver.getPageSource();
			// TODO HDFS
			FileUtils.writeStringToFile(new File(message.getTempFilePath()), sources, "utf-8", false);
		} catch (Throwable e) {
			logger.error(e.getCause().toString());
		} finally {
			if (driver != null) {
				driver.close();
				driver.quit();
			}
			return message;
		}
	}

	@SuppressWarnings("finally")
	public static KafkaMessage buildSource(KafkaMessage message) {
		String sources = "";
		WebDriver driver = null;
		try {
			driver = BrowserFactory.createWindowsPhantomJS();
			driver.get(message.getUrl());
			WebDriverWait wait = new WebDriverWait(driver, 30);
			wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));
			sources = driver.getPageSource();
			message.setType(ExplainTypeEnum._buildDocument);
			message.setContent(sources);
		} catch (Throwable e) {
			logger.error(e.getCause().toString());
		} finally {
			if (driver != null) {
				driver.close();
				driver.quit();
			}
			return message;
		}
	}

	public static void main(String[] args) throws Throwable {
		String url = "/asdfasdfas/asdf/asdf";
		System.out.println(url.matches(HtmlRegexConsts.DOMAIN));
	}

}

class ContentValue {

	public int pCount;
	public String htmlContent;

}
