package com.kd.data.docbuliders;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kd.browersearch.domain.WeiboDoc;
import com.kd.commons.http.browser.BrowserFactory;

public class WeiboDocumentBuilder {
	static Logger logger = LoggerFactory.getLogger(WeiboDocumentBuilder.class);

	/**
	 * 微信公众号 文档实体
	 * 
	 * @param url
	 * @param htmlSources
	 * @return
	 */
	public static WeiboDoc weiboDocBuild(String url, boolean isPlainText) {
		WeiboDoc doc=new WeiboDoc();
		
		WebDriver driver = null;
		try {
			String exePath="F:/chromedriver.exe";
			driver = BrowserFactory.createChrome(exePath);
			//登录
//			driver.get(url);
//			Thread.sleep(5000);
			WebDriverWait wait = new WebDriverWait(driver, 30);
//			wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));
			
			//请求个人主页
			driver.get(url);
			Thread.sleep(5000);
			wait = new WebDriverWait(driver, 30);
			wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));

			WebElement appCardEle =null;
			List<WebElement> contentList =null;
			int cardSize=0;
			int tempSize=1;
			int iBreak=0;
			do{
				appCardEle = driver.findElement(By.xpath("//div[@id='app']//div//div"));
				contentList=appCardEle.findElements(By.className("card"));
				tempSize=contentList.size();
				((JavascriptExecutor)driver).executeScript("scrollTo(0,9999)");  
			    Thread.sleep(3000L);
			    if(tempSize>cardSize){
			    	cardSize=tempSize;
			    }else{
			    	iBreak++;
			    }
			}while(iBreak<3);
			
			//FileUtils.write(new File("/data/weibo_temp"), driver.getPageSource(), "utf-8", false);
			System.out.println(cardSize);

			String author="";
			String weiboDocId="";
			String publishDate="";
			String contentText="";
			String forward="";
			String comment="";
			String like="";
			String device="";
			for(WebElement contentEle:contentList){
				author=contentEle.findElement(By.xpath("//h3[@class='m-text-cut']")).getText();
				publishDate=contentEle.findElement(By.xpath("//h4[@class='m-text-cut']//span[@class='time']")).getText();
				publishDate=getWeiboFullPublishTime(publishDate);
				device=contentEle.findElement(By.xpath("//h4[@class='m-text-cut']//span[@class='from']")).getText();
				device=getWeiboDevice(device);
				contentText=contentEle.findElement(By.xpath("//article[@class='weibo-main']//div[@class='weibo-og']//div[@class='weibo-text']")).getText();
			}
			
			String attentions=driver.findElement(By.xpath("//div[@class='mod-fil-fans']//a[contains(text(), '关注')]//span")).getText();
			String fans=driver.findElement(By.xpath("//div[@class='mod-fil-fans']//a[contains(text(), '粉丝')]//span")).getText();
			String signMind=driver.findElement(By.xpath("//div[@class='item-list']//p[@class='mod-fil-desc']")).getText();
			
		} catch (Throwable e) {
//			logger.error(e.getCause().toString());
			e.printStackTrace();
		} finally {
			if (driver != null) {
				driver.close();
				driver.quit();
			}
		}
		
		return doc;
	}
	
	public static String getWeiboFullPublishTime(String time){
		if(StringUtils.isBlank(time)){
			return "";
		}
		time=time.trim();
		if(time.matches("^[\\d]{2}-[\\d]{2}[\\s]+[\\d]{2}:[\\d]{2}[\\s]*")){
			String year=DateFormatUtils.format(new Date(), "yyyy");
			return year+"-"+time;
		}
		
		String regexToday="^[今天|today]{2,5}(.*)";
		if(time.matches(regexToday)){
			String todayYMD=DateFormatUtils.format(new Date(), "yyyy-MM-dd");
			time=todayYMD+time.replaceFirst(regexToday,"$1");
			return time;
		}
		
		return time;
	} 
	
	
	public static String getWeiboDevice(String device){
		if(StringUtils.isBlank(device)){
			return "";
		}
		return device.replaceAll("[来自|微博]{2}", "").trim();
	}
	
	
	public static void main(String[] args) {
		String url="http://m.weibo.cn/u/2775583835";
		weiboDocBuild(url,true);
		
//		String time=getWeiboFullPublishTime("今天 08:06");
//		System.out.println(time);
//		time=getWeiboFullPublishTime("02-28 19:37 ".trim());
//		System.out.println(time);
//		String device=getWeiboDevice(" 来自 Lumia 920");
//		System.out.println(device);
	}

}
