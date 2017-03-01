package com.kd.data.docbuliders;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.By;
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
			driver.get(url);
			Thread.sleep(5000);
			WebDriverWait wait = new WebDriverWait(driver, 30);
			wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("body")));
			FileUtils.write(new File("/data/weibo_temp"), driver.getPageSource(), "utf-8", false);
//			System.out.println(driver.getPageSource());
			String author=driver.findElement(By.xpath("//h1[@class='username']")).getText();
			WebElement contentElement=driver.findElement(By.xpath("//div[@id='Pl_Official_MyProfileFeed__22']//div"));
			//feed_list_item_date   attr title 
			List<WebElement> contentList=contentElement.findElements(By.xpath("//div[@action-type='feed_list_item']"));
			String weiboDocId="";
			String publishDate="";
			String contentText="";
			String forward="";
			String comment="";
			String like="";
			for(WebElement element:contentList){
				WebElement firstEle=element.findElement(By.xpath("//div[@class='WB_detail']//div[@class='WB_from']//a"));
				weiboDocId=firstEle.getAttribute("name");
				publishDate=firstEle.getAttribute("title");
				WebElement textEle=element.findElement(By.xpath("//div[@class='WB_detail']//div[@class='WB_text']"));
				contentText=textEle.getText();
				WebElement forwardEle=element.findElement(By.xpath("//div[@class='WB_feed_handle']//div[@class='WB_handle']//div[@class='WB_handle']//ul[@class='WB_row_line']//li[1]//a[@action-type='fl_forward']//span[@class='pos']//span[@node-type='forward_btn_text']//span//em[1]"));
				forward=forwardEle.getText();
				WebElement commentEle=element.findElement(By.xpath("//div[@class='WB_feed_handle']//div[@class='WB_handle']//div[@class='WB_handle']//ul[@class='WB_row_line']//li[2]//a[@action-type='fl_forward']//span[@class='pos']//span[@node-type='forward_btn_text']//span//em[1]"));
				comment=commentEle.getText();
				WebElement likeEle=element.findElement(By.xpath("//div[@class='WB_feed_handle']//div[@class='WB_handle']//div[@class='WB_handle']//ul[@class='WB_row_line']//li[3]//a[@action-type='fl_forward']//span[@class='pos']//span[@node-type='forward_btn_text']//span//em[1]"));
				like=likeEle.getText();
				System.out.println(author+"|"+weiboDocId+","+publishDate+","+contentText+","+forward+","+comment+","+like+",");
			}
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
	
	
	public static void main(String[] args) {
		String url="http://weibo.com/u/2775583835?is_hot=1";
		weiboDocBuild(url,true);
	}

}
