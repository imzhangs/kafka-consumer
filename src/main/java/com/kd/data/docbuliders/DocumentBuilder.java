package com.kd.data.docbuliders;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
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
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.enums.ExplainTypeEnum;
import com.kd.commons.http.HttpRequestUtil;
import com.kd.commons.http.browser.BrowserFactory;
import com.kd.news.domain.NewsDoc;

public class DocumentBuilder {


	public static final String shortTimeFormat="yyyy-MM-dd HH:mm";
	
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
			List<String> contentList=new ArrayList<String>();
			switch (message.getBuildDocType()) {
			case newsDoc:
				NewsDoc newsDoc = NewsDocumentBuilder.defaultNewsDocBuild(message.getUrl(), message.getContent());
				contentList.add(newsDoc.getTitle()+newsDoc.getContent());
				//indexURL ??
				indexSaveResult = HttpRequestUtil.postJSON(newsIndexSaveUrl, JSONObject.toJSONString(newsDoc));
				dbSaveResult =HttpRequestUtil.postJSON(newsSaveDB, JSONObject.toJSONString(newsDoc));
				dbSaveResult = StringUtils.isNotBlank(dbSaveResult) ? "successfully" : "failed !!";
				log.info("======================>>> weiboSaveIndex and weiboSaveDB {}  sources =>>{}", dbSaveResult, indexSaveResult);
				break;
			case topicDoc:
				BrowserSearchDoc browserSearchDoc = BrowserDocumentBuilder.browserSearchDocBuild(message.getUrl(),
						message.getContent(), true);

				contentList.add(browserSearchDoc.getTitle()+browserSearchDoc.getContent());
				//indexURL ??
				indexSaveResult = HttpRequestUtil.postJSON(newsIndexSaveUrl, JSONObject.toJSONString(browserSearchDoc));
				break;
			case weiboDoc:
				List<WeiboDoc> weibodocList = WeiboDocumentBuilder.tempWeiboDocBuild(message, true);
				
				for(WeiboDoc weiboDoc:weibodocList){
					
					contentList.add(weiboDoc.author+" "+weiboDoc.content);
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

				contentList.add(weixinGzhDoc.getTitle()+weixinGzhDoc.getContent());
				indexSaveResult = HttpRequestUtil.postJSON(weixinSaveIndex, JSONObject.toJSONString(weixinGzhDoc));
				dbSaveResult = HttpRequestUtil.postJSON(DocumentBuilder.weixinSaveDB, JSONObject.toJSONString(weixinGzhDoc));
				dbSaveResult = StringUtils.isNotBlank(dbSaveResult) ? "successfully" : "failed !!";
				log.info("======================>>>weixinSaveIndex and weixinSaveDB {}  sources =>>{}", dbSaveResult, indexSaveResult);
				break;
			case facebookDoc:
				List<FacebookDoc> listSocialAccount=FacebookDocBuilder.tempFacebookTopicBuild(message, true);
				for(FacebookDoc fb:listSocialAccount){
					contentList.add(fb.author+" "+fb.content +" "+fb.comments);
					indexSaveResult = HttpRequestUtil.postJSON(facebookSaveIndex, JSONObject.toJSONString(fb));
					dbSaveResult = HttpRequestUtil.postJSON(DocumentBuilder.facebookSaveDB, JSONObject.toJSONString(fb));
					dbSaveResult = StringUtils.isNotBlank(dbSaveResult) ? "successfully" : "failed !!";
					log.info("======================>>>facebookSaveIndex and facebookSaveDB {}  sources =>>{}", dbSaveResult, indexSaveResult);
				}
				break;
			case youtobeDoc:
				listSocialAccount=FacebookDocBuilder.tempFacebookTopicBuild(message, true);
				for(FacebookDoc youtobeDoc:listSocialAccount){
					contentList.add(youtobeDoc.author+" "+youtobeDoc.content +" "+youtobeDoc.comments);
					indexSaveResult = HttpRequestUtil.postJSON(facebookSaveIndex, JSONObject.toJSONString(youtobeDoc));
					dbSaveResult = HttpRequestUtil.postJSON(DocumentBuilder.facebookSaveDB, JSONObject.toJSONString(youtobeDoc));
					dbSaveResult = StringUtils.isNotBlank(dbSaveResult) ? "successfully" : "failed !!";
					log.info("======================>>>youtubeSaveIndex and youtubeSaveDB {}  sources =>>{}", dbSaveResult, indexSaveResult);
				}
				break;
			case twitterDoc:
				listSocialAccount=FacebookDocBuilder.tempFacebookTopicBuild(message, true);
				for(FacebookDoc twitterDoc:listSocialAccount){
					contentList.add(twitterDoc.author+" "+twitterDoc.content +" "+twitterDoc.comments);
					indexSaveResult = HttpRequestUtil.postJSON(facebookSaveIndex, JSONObject.toJSONString(twitterDoc));
					dbSaveResult = HttpRequestUtil.postJSON(DocumentBuilder.facebookSaveDB, JSONObject.toJSONString(twitterDoc));
					dbSaveResult = StringUtils.isNotBlank(dbSaveResult) ? "successfully" : "failed !!";
					log.info("======================>>>twitterSaveIndex and twitterSaveDB {}  sources =>>{}", dbSaveResult, indexSaveResult);
				}
				break;
			default:
				break;
			}
			for(String content:contentList){
				KafkaMessage dictMessage=new KafkaMessage();
				dictMessage.setTopic(remoteDicTopic);
				dictMessage.setContent(content);
				if(StringUtils.isBlank(dictMessage.getContent())){
					log.error("to dict content is empty !!! ");
				}
				HttpRequestUtil.postJSON(remoteDicSplitUrl,JSONObject.toJSONString(dictMessage) );
			}
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
	
	
	
	public static String getPublishTime(String time) throws Exception {
		try{
		if (StringUtils.isBlank(time)) {
			return  DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
		}
		time = time.trim();
		if (time.matches("[\\d]{4}-[\\d]{1,2}-[\\d]{1,2}[\\s]+[\\d]{1,2}:[\\d]{1,2}[\\s]*")) {
			return time;
		}
		
		if (time.matches("[\\d]{4}年[\\d]{1,2}月[\\d]{1,2}日[\\s]+[\\d]{1,2}:[\\d]{1,2}[\\s]*")) {
			time= time.replaceAll("[年月]{1}","-").replaceAll("日","");
			return DateFormatUtils.format(DateUtils.parseDate(time, "yyyy-M-d H:m"), DocumentBuilder.shortTimeFormat);
		}
		if (time.matches("^1[4-8][\\d]{8}$")) {
			return DateFormatUtils.format(Long.valueOf(time+"000"), DocumentBuilder.shortTimeFormat);
			
		}
		if (time.matches("^1[4-8][\\d]{11}$")) {
			return DateFormatUtils.format(Long.valueOf(time), DocumentBuilder.shortTimeFormat);
		}
		
		if (time.matches("^[\\d]{2}-[\\d]{2}[\\s]+[\\d]{2}:[\\d]{2}[\\s]*")) {
			String year = DateFormatUtils.format(new Date(), "yyyy");
			return year + "-" + time;
		}

		String regexToday = "^[今天|today]{2,5}(.*)";
		if (time.matches(regexToday)) {
			String todayYMD = DateFormatUtils.format(new Date(), "yyyy-MM-dd");
			time = todayYMD + time.replaceFirst(regexToday, "$1");
			return time;
		}


		String regexBeforeHours = "^([\\d]+)([分钟小时天周个月]{1,4})[前]*.*";
		if (time.matches(regexBeforeHours)) {
			int before=0;
			Date result=new Date();
			try{
				String hours=time.replaceFirst(regexBeforeHours, "$1");
				before=Integer.parseInt(hours);
			}catch(Throwable e){};
			

			String mount=time.replaceFirst(regexBeforeHours, "$2");
			if(mount.contains("小时")){
				result=DateUtils.addHours(new Date(), -before);
			}else if(mount.contains("天")){
				result=DateUtils.addDays(new Date(), -before);
			}else if(mount.contains("周")){
				result=DateUtils.addWeeks(new Date(), -before);
			}else if(mount.matches("[个]?月")){
				result=DateUtils.addMonths(new Date(), -before);
			}else{
				result=DateUtils.addMinutes(new Date(), -before);
			}
			
			return DateFormatUtils.format(result, "yyyy-MM-dd HH:mm:ss");
		}

		String regexNow = "^刚刚.*";
		if (time.matches(regexNow)) {
			String todayYMDHMS = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
			return todayYMDHMS;
		}

		String regexMin = "^([\\d]+)月([\\d]+)日[\\s]+([\\d]{2}:[\\d]{2}[\\s]*).*";
		if (time.matches(regexMin)) {
			String year = DateFormatUtils.format(new Date(), "yyyy");
			String todayYMD =time.replaceAll(regexMin, year+"-$1-$2 $3");
			return todayYMD;
		}

		regexMin = "^([\\d]+).*";
		if (time.matches(regexMin)) {
			int beforeMin = Integer.valueOf(time.replaceFirst(regexMin, "$1"));
			Date date = DateUtils.addMinutes(new Date(), -beforeMin);
			String todayYMD = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss");
			return todayYMD;
		}
		regexMin = "^([\\d]+).*";
		if (time.matches(regexMin)) {
			int beforeMin = Integer.valueOf(time.replaceFirst(regexMin, "$1"));
			Date date = DateUtils.addMinutes(new Date(), -beforeMin);
			String todayYMD = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss");
			return todayYMD;
		}
		
		}catch(Throwable e){time=DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");}
		return time;
		
	}
	

	public static void main(String[] args) throws Throwable {
		String times = "1个月前";
		System.out.println(getPublishTime(times));
	}

}

class ContentValue {

	public int pCount;
	public String htmlContent;

}
