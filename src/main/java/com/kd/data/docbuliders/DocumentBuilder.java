package com.kd.data.docbuliders;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.kd.browersearch.domain.BrowserSearchDoc;
import com.kd.browersearch.domain.FacebookDoc;
import com.kd.browersearch.domain.WeiboDoc;
import com.kd.commons.consts.HtmlRegexConsts;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.enums.ExplainTypeEnum;
import com.kd.commons.http.HttpRequestUtil;
import com.kd.commons.http.browser.BrowserFactory;
import com.kd.news.domain.NewsDoc;

public class DocumentBuilder {

	static Logger log = LoggerFactory.getLogger(DocumentBuilder.class);

	public static String weiboSaveIndex;
	public static String weixinSaveIndex;
	public static String weiboSaveDB;
	public static String weixinSaveDB;
	public static String facebookSaveIndex;
	public static String facebookSaveDB;

	public static String newsIndexSaveUrl;
	public static String newsSaveDB;
	
	public static String phantomJSPath;
	public static String windowsPhantomJSPath;
	
	public static String remoteDicSplitUrl;
	public static String remoteDicTopic;
	
	public static WebDriver getWebDriver(){
		if(System.getProperty("os.name").toLowerCase().contains("windows")){
			phantomJSPath=windowsPhantomJSPath;
		}
		return  BrowserFactory.createPhantomJS(phantomJSPath);
	}
	

	public static void docBuilderAndSave(KafkaMessage message) {
		if (message == null) {
			return;
		}
		if (message.getUrl() == null) {
			return;
		}
		if (message.getBuildDocType() == null) {
			return;
		}

		String indexSaveResult ="";
		String dbSaveResult = "";
		if ( message.getBuildDocType() != null) {
			
			//// source 获取
			switch (message.getBuildDocType()) {
			case newsDoc:
			case topicDoc:
			case weixinGzhDoc:
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
				}
				
				if (StringUtils.isBlank(message.getContent())) {
					buildSource(message);
				}
				break;
			default:
				break;
			}
		
			
			
			//// 存档
			String content="";
			switch (message.getBuildDocType()) {
			case newsDoc:
				NewsDoc newsDoc = NewsDocumentBuilder.defaultNewsDocBuild(message.getUrl(), message.getContent());
				content=newsDoc.getContent();
				//indexURL ??
				indexSaveResult = HttpRequestUtil.postJSON(newsIndexSaveUrl, JSONObject.toJSONString(newsDoc));
				dbSaveResult =HttpRequestUtil.postJSON(newsSaveDB, JSONObject.toJSONString(newsDoc));
				dbSaveResult = StringUtils.isNotBlank(dbSaveResult) ? "successfully" : "failed !!";
				log.info("======================>>> weiboSaveIndex and weiboSaveDB {}  sources =>>{}", dbSaveResult, indexSaveResult);
				break;
			case topicDoc:
				BrowserSearchDoc browserSearchDoc = BrowserDocumentBuilder.browserSearchDocBuild(message.getUrl(),
						message.getContent(), true);

				content=browserSearchDoc.getContent();
				//indexURL ??
				indexSaveResult = HttpRequestUtil.postJSON(newsIndexSaveUrl, JSONObject.toJSONString(browserSearchDoc));
				break;
			case weiboDoc:
				List<WeiboDoc> weibodocList = WeiboDocumentBuilder.tempWeiboDocBuild(message, true);
				
				for(WeiboDoc weiboDoc:weibodocList){
					
					indexSaveResult = HttpRequestUtil.postJSON(weiboSaveIndex, JSONObject.toJSONString(weiboDoc));
					dbSaveResult =HttpRequestUtil.postJSON(weiboSaveDB, JSONObject.toJSONString(weiboDoc));
					dbSaveResult = StringUtils.isNotBlank(dbSaveResult) ? "successfully" : "failed !!";
					log.info("======================>>> weiboSaveIndex and weiboSaveDB {}  sources =>>{}", dbSaveResult, indexSaveResult);
				}
				break;
			case weixinGzhDoc:
				BrowserSearchDoc weixinGzhDoc = WeixinGzhDocumentBuilder.browserWeixinGzhDocBuild(message, true);
				if(weixinGzhDoc==null){
					break;
				}

				content=weixinGzhDoc.getContent();
				indexSaveResult = HttpRequestUtil.postJSON(weixinSaveIndex, JSONObject.toJSONString(weixinGzhDoc));
				dbSaveResult = HttpRequestUtil.postJSON(DocumentBuilder.weixinSaveDB, JSONObject.toJSONString(weixinGzhDoc));
				dbSaveResult = StringUtils.isNotBlank(dbSaveResult) ? "successfully" : "failed !!";
				log.info("======================>>>weixinSaveIndex and weixinSaveDB {}  sources =>>{}", dbSaveResult, indexSaveResult);
				break;
			case facebookDoc:
				List<FacebookDoc> listFb=FacebookDocBuilder.tempFacebookTopicBuild(message, true);
				for(FacebookDoc fb:listFb){
					indexSaveResult = HttpRequestUtil.postJSON(facebookSaveIndex, JSONObject.toJSONString(fb));
					dbSaveResult = HttpRequestUtil.postJSON(DocumentBuilder.facebookSaveDB, JSONObject.toJSONString(fb));
					dbSaveResult = StringUtils.isNotBlank(dbSaveResult) ? "successfully" : "failed !!";
					log.info("======================>>>facebookSaveIndex and facebookSaveDB {}  sources =>>{}", dbSaveResult, indexSaveResult);
				}
				break;
			default:
				break;
			}
			KafkaMessage dictMessage=new KafkaMessage();
			dictMessage.setTopic(remoteDicTopic);
			dictMessage.setContent(content);
			
			HttpRequestUtil.postJSON(remoteDicSplitUrl,JSONObject.toJSONString(dictMessage) );
		} else {
			log.error("message content or buildDocType is null ....");
		}

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
			log.error(e.getCause().toString());
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
		WebDriver driver = getWebDriver();
		try {
			driver.get(message.getUrl());
			WebDriverWait wait = new WebDriverWait(driver, 30);
			wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));
			sources = driver.getPageSource();
			message.setType(ExplainTypeEnum._buildDocument);
			message.setContent(sources);
		} catch (Throwable e) {
			log.error(e.getCause().toString());
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
