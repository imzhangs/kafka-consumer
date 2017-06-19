package com.kd.data.docbuliders;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;
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
	@Deprecated
	@SuppressWarnings("finally")
	public static List<WeiboDoc> cnWeiboDocBuild(KafkaMessage message, boolean isPlainText) {
		String url=message.getUrl();
		List<WeiboDoc> weiboList=new ArrayList<>();
		WebDriver driver = null;
		try {
			driver = DocumentBuilder.getWebDriver();
			//请求个人主页
			driver.get(url);
			WebDriverWait wait = new WebDriverWait(driver, 30);
			wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));
			Thread.sleep(10000);
			WebElement appCardEle =null;
			
			appCardEle = driver.findElement(By.xpath("//html//body//div[@id='app']"));
			System.out.println(appCardEle.getAttribute("innerHTML"));
			appCardEle=appCardEle.findElement(By.tagName("div")).findElement(By.tagName("div"));
			List<WebElement> contentList = appCardEle.findElements(By.className("m-panel"));

			int cardSize = 0;
			int tempSize = 1;
			int iBreak = 0;
			do {
				try{
					appCardEle = driver.findElement(By.xpath("//html//body//div[@id='app']//div//div"));
					contentList = appCardEle.findElements(By.className("m-panel"));
					tempSize = contentList.size();
					((JavascriptExecutor) driver).executeScript("scrollTo(0,999)");
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
				publishDate = DocumentBuilder.getPublishTime(publishDate);
				device = node.select("h4[class=m-text-cut]>span[class=from]").text();
				device = getWeiboDevice(device);
				contentText = node.select("article[class=weibo-main]>div[class=weibo-og]>div[class=weibo-text]").text();
				contentText=contentText.replaceAll("展开全文","");
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
					authorId=Integer.valueOf(author.hashCode());
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
	
	/**
	 * 对  http://weibo.com/u/xxxx 解析有效
	 * 
	 * @param url
	 * @param htmlSources
	 * @return
	 */
	@SuppressWarnings("finally")
	public static List<WeiboDoc> comWeiboDocBuild(KafkaMessage message, boolean isPlainText) {

		String url=message.getUrl();
		List<WeiboDoc> weiboList=new ArrayList<>();
		WebDriver driver =null;
		try {
			driver =DocumentBuilder.getWebDriver();
			//请求个人主页
			driver.get("https://login.sina.com.cn/signup/signin.php");
//			driver.get("https://passport.weibo.cn/signin/login");
			WebDriverWait wait = new WebDriverWait(driver, 30);
			wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));
			Thread.sleep(RandomUtils.nextInt(3000)+1000);
			WebElement elUsername=driver.findElement(By.xpath("//form[@id='vForm']//input[@id='username']"));
			System.out.println(elUsername.getTagName());
			elUsername.clear();
			Thread.sleep(RandomUtils.nextInt(1000)+1000);
			elUsername.sendKeys("13728878827");
			Thread.sleep(RandomUtils.nextInt(1000)+1000);
			System.out.println("username==>:" + elUsername.getAttribute("value"));
			WebElement elPass=driver.findElement(By.xpath("//form[@id='vForm']//input[@id='password']"));
			elPass.clear();
			Thread.sleep(RandomUtils.nextInt(1000)+1000);
			elPass.sendKeys("Oiu3171211");
			Thread.sleep(RandomUtils.nextInt(1000)+1000);
			System.out.println("username==>:" + elPass.getAttribute("value"));
			driver.findElement(By.xpath("//form[@id='vForm']//input[@type='submit']")).click();
			Thread.sleep(RandomUtils.nextInt(10000)+5000);
			System.out.println(driver.getTitle());
			
			driver.get("http://weibo.com/u/3511885300");
			Thread.sleep(RandomUtils.nextInt(10000)+5000);
			wait = new WebDriverWait(driver, RandomUtils.nextInt(30)+30);
			wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));
			System.out.println(driver.getTitle());
			FileUtils.writeStringToFile(new File("/data/htmltemp/123"),driver.getPageSource()+"\n","utf-8",false);
			Document htmlDoc=Jsoup.parse(driver.getPageSource());
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
				
				Elements trList = htmlDoc.select("div[id=Pl_Core_T8CustomTriColumn__3]>table[class=tb_counter]>tbody>tr>");
				Element tr=trList.get(0);
				attentions = tr.select("td[class=S_line1]:eq[0]>a>strong").text();
						
			}catch(Throwable e){
				e.printStackTrace();
			}
			
			try{
				fans = htmlDoc.select("tr>td[class=S_line1]:eq[1]>a>strong").text();
			}catch(Throwable e){}
			try{
				signMind =htmlDoc.select("tr>td[class=S_line1]:eq[2]>a>strong").text();
			}catch(Throwable e){}
			
			Elements els = htmlDoc.getElementsByClass("m-panel");
			System.out.println(els.size());
			for (Element node : els) {
				author = node.select("header>div>div>a>h3[class=m-text-cut]").text();
				publishDate = node.select("h4[class=m-text-cut]>span[class=time]").text();
				publishDate = DocumentBuilder.getPublishTime(publishDate);
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
				Date publishedDate=DateUtils.parseDate(publishDate, DocumentBuilder.shortTimeFormat);
				
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
				weiboDoc.setGroupId(DateFormatUtils.format(publishedDate, StringFormatConsts.DATE_NUMBER_FORMAT));
				weiboDoc.setSignMind(signMind);
				weiboDoc.setUrl(url);
				weiboDoc.setJobId(30552);
				weiboDoc.setDate(publishedDate.getTime());
				weiboDoc.setJobId(message.getSourceId());
				weiboDoc.setLevel(message.getLevel());
				weiboDoc.setType(message.getTypeId());
				
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
	
	
	@SuppressWarnings("finally")
	public static List<WeiboDoc> tempWeiboDocBuild(KafkaMessage message, boolean isPlainText) {
		List<WeiboDoc> weiboList=new ArrayList<>();
		if(message==null || StringUtils.isBlank(message.getContent())){
			logger.error("message content is null !!!!");
			return weiboList;
		}
		String url=message.getUrl();
		try {
			Document htmlDoc=Jsoup.parse(message.getContent());
			String author = "";
			String weiboDocId = "";
			String publishDate = "";
			String contentText = "";
			int forward = 0;
			int comment = 0;
			int like = 0;
			String subUrl = "";
			String device = "";
			int authorId=0;
			String attentions = "0";
			String fans = "0";
			String signMind = "0";
			Elements trList =null;
			try{
				
				Element myAttr = htmlDoc.getElementById("Pl_Core_T8CustomTriColumn__3");
				trList =myAttr.select("table[class=tb_counter]");
				Element tr=trList.get(0);
				attentions = tr.select("td:eq(0)>a[class=t_link S_txt1]>strong[class=W_f18]").text();
			}catch(Throwable e){
				logger.error("table[class=tb_counter] is null");
			}
			
			try{
				Element tr=trList.get(1);
				fans = tr.select("td:eq(1)>a[class=t_link S_txt1]>strong[class=W_f18]").text();
			}catch(Throwable e){}
			try{
				signMind =htmlDoc.select("div[class=pf_intro]").get(0).text();
			}catch(Throwable e){}
			
			Elements cardList = htmlDoc.select("div[class=WB_cardwrap WB_feed_type S_bg2]");
			for (Element node : cardList) {
				author = node.select("div[class=WB_info]").text();
				Elements publishEle=node.select("div[class=WB_detail]>div[class=WB_from S_txt2]>a:eq(0)");
				
				publishDate = publishEle.attr("title").trim();
				publishDate = DocumentBuilder.getPublishTime(publishDate);
				
				subUrl="http://weibo.com"+publishEle.attr("href");
				weiboDocId = MD5Util.MD5(subUrl);
						
				device = node.select("div[class=WB_detail]>div[class=WB_from S_txt2]>a:eq(1)").text();
				device = getWeiboDevice(device);
				contentText=node.select("div[class=WB_text W_f14]").text();
				
				contentText+=node.select("div[class=WB_expand S_bg1]").text();
				String forwardText = node.select("ul[class=WB_row_line WB_row_r4 clearfix S_line2]>li:eq(1)").text();
				forwardText=forwardText.replaceAll("[^\\d]*(\\d+).*", "$1");
				
				String commentText = node.select("ul[class=WB_row_line WB_row_r4 clearfix S_line2]>li:eq(2)").text();
				commentText=commentText.replaceAll("[^\\d]*(\\d+).*", "$1");
				
				String likeText = node.select("ul[class=WB_row_line WB_row_r4 clearfix S_line2]>li:eq(3)").text();
				likeText=likeText.replaceAll("[^\\d]*(\\d+).*", "$1");
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
				weiboDoc.setUrl(subUrl);
				weiboDoc.setJobId(30552);
				weiboDoc.setDate(Long.valueOf(DateFormatUtils.format(new Date(), StringFormatConsts.DATE_HOUR_NUMBER_FORMAT)));
				weiboDoc.setSource(message.getSourceId());
				weiboDoc.setLevel(message.getLevel());
				weiboDoc.setType(message.getTypeId());
				
				weiboList.add(weiboDoc);
			}
			
		} catch (Throwable e) {
			// logger.error(e.getCause().toString());
			e.printStackTrace();
		} finally {
			return weiboList;
		}
	}

	
	public static String getWeiboDevice(String device) {
		if (StringUtils.isBlank(device)) {
			return "";
		}
		return device.replaceAll("[来自|微博]{2}", "").trim();
	}

	public static void main(String[] args) throws Throwable {
//		String url = "http://weibo.com/u/2775583835";
//		KafkaMessage message=new KafkaMessage();
//		message.setContent(FileUtils.readFileToString(new File("F:/data/htmltemp/123"), "utf-8"));
//		message.setUrl(url);
//		List<WeiboDoc> list=tempWeiboDocBuild(message, true);
//		for(WeiboDoc doc:list){
//			System.out.println(JSONObject.toJSONString(doc));
//		}
		
		System.out.println(DocumentBuilder.getPublishTime("3月19日 21:51"));
	}

}
