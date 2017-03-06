package com.kd.data.docbuliders;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kd.browersearch.domain.WeiboDoc;
import com.kd.commons.consts.StringFormatConsts;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.enums.BuildDocTypeEnum;
import com.kd.commons.utils.MD5Util;

public class WeiboDocumentBuilder {
	static Logger logger = LoggerFactory.getLogger(WeiboDocumentBuilder.class);

	/**
	 * 对  http://m.weibo.cn/u/xxxx 解析有效
	 * 
	 * @param url
	 * @param htmlSources
	 * @return
	 */
	@SuppressWarnings("finally")
	public static List<WeiboDoc> weiboDocBuild(KafkaMessage message, boolean isPlainText) {
		String url=message.getUrl();
		List<WeiboDoc> weiboList=new ArrayList<>();
		WebDriver driver = null;
		try {
			driver = DocumentBuilder.getWebDriver();
			//请求个人主页
			driver.get(url);
			Thread.sleep(10000);
			WebDriverWait wait = new WebDriverWait(driver, 30);
			wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));
			
			WebElement appCardEle =null;
			
			appCardEle = driver.findElement(By.xpath("//div[@id='app']//div//div"));
			List<WebElement> contentList = appCardEle.findElements(By.className("m-panel"));

			int cardSize = 0;
			int tempSize = 1;
			int iBreak = 0;
			do {
				try{
					appCardEle = driver.findElement(By.xpath("//div[@id='app']//div//div"));
					contentList = appCardEle.findElements(By.className("m-panel"));
					tempSize = contentList.size();
					((JavascriptExecutor) driver).executeScript("scrollTo(0,9999)");
					Thread.sleep(5000L);
					if (tempSize > cardSize) {
						cardSize = tempSize;
					} else {
						iBreak++;
					}
				}catch(Throwable e){continue;}
			} while(iBreak<3);

			FileUtils.write(new File("/data/weiboTemp/" + MD5Util.MD5(url)), appCardEle.getAttribute("innerHTML"),
					"utf-8", false);
			// System.out.println(appCardEle.getAttribute("innerHTML"));

			String author = "";
			String weiboDocId = "";
			String publishDate = "";
			String contentText = "";
			int forward = 0;
			int comment = 0;
			int like = 0;
			String device = "";
			int authorId=0;
			String attentions = "0";
			String fans = "0";
			String signMind = "0";
			try{
				attentions = driver
						.findElement(By.xpath("//div[@class='mod-fil-fans']//a[contains(text(), '关注')]//span")).getText();
			}catch(Throwable e){}

			try{
				fans = driver.findElement(By.xpath("//div[@class='mod-fil-fans']//a[contains(text(), '粉丝')]//span")).getText();
			}catch(Throwable e){}
			try{
			 signMind = appCardEle
					.findElement(By.xpath("//div[@class='profile-cover']//div[@class='item-list']//p")).getText();
			}catch(Throwable e){}

			Document doc = Jsoup.parse(appCardEle.getAttribute("innerHTML"));
			Elements els = doc.getElementsByClass("m-panel");
			System.out.println(els.size());
			for (Element node : els) {
				author = node.select("header>div>div>a>h3[class=m-text-cut]").text();
				publishDate = node.select("h4[class=m-text-cut]>span[class=time]").text();
				publishDate = getWeiboFullPublishTime(publishDate);
				device = node.select("h4[class=m-text-cut]>span[class=from]").text();
				device = getWeiboDevice(device);
				contentText = node.select("article[class=weibo-main]>div[class=weibo-og]>div[class=weibo-text]").text();
				String forwardText = node.select("footer>div:eq(0)>h4").text();
				String commentText = node.select("footer>div:eq(2)>h4").text();
				String likeText = node.select("footer>div:eq(4)>h4").text();
				
				if(StringUtils.isBlank(contentText) && StringUtils.isBlank(author) && StringUtils.isBlank(publishDate)){
					continue;
				}
				
				try {
					forward = Integer.valueOf(forwardText);
				} catch (Throwable e) {
				}
				try {
					comment = Integer.valueOf(commentText);
				} catch (Throwable e) {
				}
				try {
					like = Integer.valueOf(likeText);
				} catch (Throwable e) {
				}
				
				try {
					String authorIdText=url.substring(url.lastIndexOf("/"),url.length());
					authorId=Integer.valueOf(authorIdText);
				} catch (Throwable e) {
				}
				
				
				WeiboDoc weiboDoc=new WeiboDoc();
				
				try {
					weiboDoc.setAttentionsCount(Integer.valueOf(attentions));
				} catch (Throwable e) {
				}
				try {
					weiboDoc.setFansCount(Integer.valueOf(fans));
				} catch (Throwable e) {
				}

				String docText=author + "," + weiboDocId + "," + publishDate + "," + contentText ;
				weiboDocId=StringUtils.isBlank(weiboDocId)?MD5Util.MD5(docText):"";
				
				weiboDoc.setId(weiboDocId);
				weiboDoc.setAuthor(author);
				weiboDoc.setAuthorId(authorId);
				weiboDoc.setClickNum(like);
				weiboDoc.setCommentNum(comment);
				weiboDoc.setForwardNum(forward);
				weiboDoc.setPublishDate(publishDate);
				weiboDoc.setContent(contentText);
				weiboDoc.setDevice(device);
				weiboDoc.setDocType(BuildDocTypeEnum.weiboDoc);
				weiboDoc.setGroupId(DateFormatUtils.format(new Date(), StringFormatConsts.DATE_NUMBER_FORMAT));
				weiboDoc.setSignMind(signMind);
				weiboDoc.setUrl(url);
				weiboDoc.setJobId(30552);
				weiboDoc.setDate(Long.valueOf(DateFormatUtils.format(new Date(), StringFormatConsts.DATE_HOUR_NUMBER_FORMAT)));
				weiboDoc.setJobId(message.getSourceId());
				weiboDoc.setLevel(message.getLevel());
				
				weiboList.add(weiboDoc);
			}

		} catch (Throwable e) {
			// logger.error(e.getCause().toString());
			e.printStackTrace();
		} finally {
			if (driver != null) {
				driver.close();
				driver.quit();
			}
			return weiboList;
		}
		
	}

	public static String getWeiboFullPublishTime(String time) {
		if (StringUtils.isBlank(time)) {
			return "";
		}
		time = time.trim();
		if (time.matches("[\\d]{4}-[\\d]{2}-[\\d]{2}[\\s]+[\\d]{2}:[\\d]{2}[\\s]*")) {
			return time;
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

		String regexMin = "^([\\d]+).*";
		if (time.matches(regexMin)) {
			int beforeMin = Integer.valueOf(time.replaceFirst(regexMin, "$1"));
			Date date = DateUtils.addMinutes(new Date(), -beforeMin);
			String todayYMD = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss");
			return todayYMD;
		}

		return time;
	}

	public static String getWeiboDevice(String device) {
		if (StringUtils.isBlank(device)) {
			return "";
		}
		return device.replaceAll("[来自|微博]{2}", "").trim();
	}

	public static void main(String[] args) {
//		String url = "http://m.weibo.cn/u/2775583835";
//		KafkaMessage message=new KafkaMessage();
//		
//		message.setUrl(url);
//		weiboDocBuild(message, true);

		// String time=getWeiboFullPublishTime("16分钟前");
		// System.out.println(time);
		// time=getWeiboFullPublishTime("02-28 19:37 ".trim());
		// System.out.println(time);
		// String device=getWeiboDevice(" 来自 Lumia 920");
		// System.out.println(device);

	}

}
