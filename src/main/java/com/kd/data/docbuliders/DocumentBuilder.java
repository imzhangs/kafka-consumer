package com.kd.data.docbuliders;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.WebDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.kd.browersearch.domain.BrowserSearchDoc;
import com.kd.commons.consts.HtmlContentConsts;
import com.kd.commons.consts.HtmlRegexConsts;
import com.kd.commons.consts.StringFormatConsts;
import com.kd.commons.domain.AbstractDocument;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.enums.ExplainTypeEnum;
import com.kd.commons.http.browser.BrowserFactory;
import com.kd.commons.utils.MD5Util;
import com.kd.news.domain.NewsDoc;

@Component
public class DocumentBuilder {

	static Logger logger = LoggerFactory.getLogger(DocumentBuilder.class);

	static final String NEWSDOC_KEYWORDS[] = { "评论", "参与" };

	/**
	 * 默认html解析方案生成 新闻文档实体
	 * 
	 * @param url
	 * @param htmlSources
	 * @return
	 */
	public static NewsDoc defaultNewsDocBuild(String url, String htmlSources) {
		if (StringUtils.isBlank(htmlSources)) {
			return null;
		}
		NewsDoc newsDoc = new NewsDoc();
		Document doc = Jsoup.parse(htmlSources);
		// title
		String title = doc.title();
		try {
			if (StringUtils.isBlank(title)) {
				title = htmlSources.split(HtmlRegexConsts.TITLE_S)[1].split(HtmlRegexConsts.TITLE_E)[0];
			}
		} catch (Throwable e) {
		}

		// publish datetime
		String publishTime = doc.body().data().replaceFirst(HtmlRegexConsts.PUBLISH_DATE_TIME_MATCHE, "$1");
		publishTime = publishTime.matches(HtmlRegexConsts.PUBLISH_DATE_TIME_MATCHE) ? publishTime
				: doc.body().text().replaceFirst(HtmlRegexConsts.PUBLISH_DATE_TIME_MATCHE, "$1");

		Map<String, ContentValue> contentMapping = new HashMap<String, ContentValue>();

		// 猜测正文，p标签统计 start====================>
		for (Element paperSub : doc.body().select("p").next()) {
			String key = MD5Util.MD5(paperSub.parent().html());
			ContentValue value = new ContentValue();
			if (contentMapping.containsKey(key)) {
				value = contentMapping.get(key);
				value.pCount += 1;
			} else {
				value.pCount = 1;
				value.htmlContent = paperSub.parent().text().trim();
			}
			contentMapping.put(key, value);
		}

		// 正文最大可能:获得最多p标签所在区域 的html文本
		int maxCount = 0;
		String maxKey = "";
		for (Map.Entry<String, ContentValue> entry : contentMapping.entrySet()) {
			if (null != entry.getValue() && entry.getValue().pCount > maxCount) {
				maxCount = entry.getValue().pCount;
				maxKey = entry.getKey();
			}
		}

		StringBuffer contentBuff = new StringBuffer();
		if (StringUtils.isNotBlank(maxKey)) {
			logger.info("正文最大概率 区域Key:" + maxKey + ",<p> maxCount:" + maxCount);
			ContentValue value = contentMapping.get(maxKey);
			contentBuff.setLength(0);
			contentBuff.append(value.htmlContent);
		}
		/// <<<=========正文统计结束==========

		String commentsCount = "";
		String partationCount = "";

		int partationShortest = doc.text().length();
		Element partationElementF = null;
		Element partationElementFF = null;
		logger.info("<--------【参与数】正则   && 猜测 选最短--------->");
		Elements partationElements = doc.getElementsMatchingText(".*参与.*");
		for (Element e : partationElements) {
			if (e.text().length() < partationShortest && e.text().matches(HtmlRegexConsts.TEXT_NUMBER_MATCHES)) {
				partationShortest = e.text().length();
				partationElementF = e.parent();
				partationElementFF = partationElementF.parent();
				partationCount = e.text();
			}
		}

		if (partationCount.matches(HtmlRegexConsts.TEXT_NUMBER_MATCHES)) {
			partationCount = partationCount.replaceFirst(HtmlRegexConsts.TEXT_NUMBER_REPLACES, "$2");
			logger.info("<-------- 【参与数】   获得--------->:{}", partationCount);
		}

		if (StringUtils.isBlank(partationCount) && partationElementF != null) {
			partationCount = partationElementF.text();
			partationCount = partationCount.replaceFirst(HtmlRegexConsts.TEXT_NUMBER_REPLACES, "$2");
			logger.info("<-------- 【参与数】   猜测  1--------->:{}", partationCount);
		}

		if (StringUtils.isBlank(partationCount) && partationElementFF != null) {
			partationCount = partationElementFF.text();
			partationCount = partationCount.replaceFirst(HtmlRegexConsts.TEXT_NUMBER_REPLACES, "$2");
			logger.info("<-------- 【参与数】   猜测  2--------->:{}", partationCount);
		}

		int commentsShortest = doc.text().length();
		int maxCommentsCountInt = 0;
		int commentsCountInt = 0;
		Element commentsElementF = null;
		Element commentsElementFF = null;
		logger.info("<-------- 【评论数】   正则&& 猜测 选最短--------->");
		Elements commentsElements = doc.getElementsMatchingText(".*评论.*");
		for (Element e : commentsElements) {
			if (e.text().length() < commentsShortest && e.text().matches(HtmlRegexConsts.TEXT_NUMBER_MATCHES)) {
				commentsShortest = e.text().length();
				commentsCount = e.text();
				commentsElementF = e.parent();
				commentsElementFF = commentsElementF.parent();
				if (commentsCount.matches(HtmlRegexConsts.TEXT_NUMBER_MATCHES)) {
					commentsCount = commentsCount.replaceFirst(HtmlRegexConsts.TEXT_NUMBER_REPLACES, "$2");
					try {
						commentsCountInt = Integer.valueOf(commentsCount);
						maxCommentsCountInt = commentsCountInt > maxCommentsCountInt ? commentsCountInt
								: maxCommentsCountInt;
					} catch (Throwable cce) {
					}
				}
			}
		}

		logger.info("<-------- 【评论数】   获得 --------->:{}", commentsCount);

		if (maxCommentsCountInt <= 0 && commentsElementF != null) {
			commentsCount = commentsElementF.text();
			commentsCount = commentsCount.replaceFirst(HtmlRegexConsts.TEXT_NUMBER_REPLACES, "$2");
			try {
				commentsCountInt = Integer.valueOf(commentsCount);
				maxCommentsCountInt = commentsCountInt > maxCommentsCountInt ? commentsCountInt : maxCommentsCountInt;
			} catch (Throwable cce) {
			}
			logger.info("<-------- 【评论数】   猜测  1--------->:{}", maxCommentsCountInt);
		}

		if (maxCommentsCountInt <= 0 && commentsElementFF != null) {
			commentsCount = commentsElementFF.text();
			commentsCount = commentsCount.replaceFirst(HtmlRegexConsts.TEXT_NUMBER_REPLACES, "$2");
			try {
				commentsCountInt = Integer.valueOf(commentsCount);
				maxCommentsCountInt = commentsCountInt > maxCommentsCountInt ? commentsCountInt : maxCommentsCountInt;
			} catch (Throwable cce) {
			}
			logger.info("<-------- 【评论数】   猜测  2--------->:{}", maxCommentsCountInt);
		}

		try {
			partationCount = partationCount.matches("\\d+") ? partationCount : "0";
		} catch (Throwable e) {
		}

		// author
		String author = url.replaceFirst(HtmlRegexConsts.DOMAIN, "$2");

		// groupId
		String groupId = DateFormatUtils.format(new Date(), StringFormatConsts.DATE_HOUR_NUMBER_FORMAT);
		String urlMD5 = MD5Util.MD5(url);

		// updateTime
		String updateTime = DateFormatUtils.format(new Date(), StringFormatConsts.SIMPLE_DATETIME_FORMAT);

		String content = contentBuff.toString();
		// content.2
		content = content.split(HtmlContentConsts.CONTENT_END)[0];
		// content.3
		content = content.replaceAll(HtmlContentConsts.CONTENT_INNER_MEDIA, "");
		// content.4
		content = content.replaceAll(HtmlRegexConsts.EXHANGE_TAG, "");

		logger.info("result:Start==================<{}>===================", url);
		logger.info(" url:\t\t {}", url);
		logger.info(" publishTime:\t\t {}", publishTime);
		logger.info(" title:\t\t {}", title);
		logger.info(" author:\t\t {}", author);
		logger.info(" groupId:\t\t {}", groupId);
		logger.info(" urlMD5:\t\t {}", urlMD5);
		logger.info(" updateTime:\t\t {}", updateTime);
		logger.info(" commentsCount:\t\t {}", maxCommentsCountInt);
		logger.info(" partationCount:\t\t {}", partationCount);
		logger.info(" content:\t\t {}", content.length() > 64 ? content.substring(0, 64) + "......" : content);
		logger.info("result:End==================================content.length={}", content.length());

		// 装箱
		newsDoc.setAuthor(author);
		newsDoc.setContent(StringUtils.isBlank(content) ? title : content);
		try {
			Date publishDate = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(publishTime);
			publishTime = DateFormatUtils.format(publishDate, "yyyyMMddHH");
		} catch (ParseException e) {
			publishTime = DateFormatUtils.format(new Date(), "yyyyMMddHH");
		}
		newsDoc.setDate(publishTime);
		newsDoc.setUpdateTime(updateTime);
		newsDoc.setGroupId(groupId);
		newsDoc.setId(groupId + urlMD5);
		newsDoc.setCommentNums(maxCommentsCountInt);
		newsDoc.setPartationNums(Integer.valueOf(partationCount));
		newsDoc.setTitle(title);
		newsDoc.setTypeId(0); // TODO ??
		newsDoc.setUrl(url);
		return newsDoc;
	}

	/**
	 * 默认html 浏览器搜索文档实体
	 * 
	 * @param url
	 * @param htmlSources
	 * @return
	 */
	public static BrowserSearchDoc browserSearchDocBuild(String url, String htmlSources) {
		if (StringUtils.isBlank(htmlSources)) {
			return null;
		}
		BrowserSearchDoc browserSearchDoc = new BrowserSearchDoc();
		Document doc = Jsoup.parse(htmlSources);
		// title
		String title = doc.title();
		try {
			if (StringUtils.isBlank(title)) {
				title = htmlSources.split(HtmlRegexConsts.TITLE_S)[1].split(HtmlRegexConsts.TITLE_E)[0];
			}
		} catch (Throwable e) {
		}

		// publish datetime
		String publishTime = doc.body().data().replaceFirst(HtmlRegexConsts.PUBLISH_DATE_TIME_MATCHE, "$1");
		publishTime = publishTime.matches(HtmlRegexConsts.PUBLISH_DATE_TIME_MATCHE) ? publishTime
				: doc.body().text().replaceFirst(HtmlRegexConsts.PUBLISH_DATE_TIME_MATCHE, "$1");

		Map<String, ContentValue> contentMapping = new HashMap<String, ContentValue>();

		// 猜测正文，p标签统计 start====================>
		for (Element paperSub : doc.body().select("p").next()) {
			String key = MD5Util.MD5(paperSub.parent().html());
			ContentValue value = new ContentValue();
			if (contentMapping.containsKey(key)) {
				value = contentMapping.get(key);
				value.pCount += 1;
			} else {
				value.pCount = 1;
				value.htmlContent = paperSub.parent().text().trim();
			}
			contentMapping.put(key, value);
		}

		// 正文最大可能:获得最多p标签所在区域 的html文本
		int maxCount = 0;
		String maxKey = "";
		for (Map.Entry<String, ContentValue> entry : contentMapping.entrySet()) {
			if (null != entry.getValue() && entry.getValue().pCount > maxCount) {
				maxCount = entry.getValue().pCount;
				maxKey = entry.getKey();
			}
		}

		StringBuffer contentBuff = new StringBuffer();
		if (StringUtils.isNotBlank(maxKey)) {
			logger.info("正文最大概率 区域Key:" + maxKey + ",<p> maxCount:" + maxCount);
			ContentValue value = contentMapping.get(maxKey);
			contentBuff.setLength(0);
			contentBuff.append(value.htmlContent);
		}
		/// <<<=========正文统计结束==========

		// author
		String author = url.replaceFirst(HtmlRegexConsts.DOMAIN, "$2");

		// groupId
		String groupId = DateFormatUtils.format(new Date(), StringFormatConsts.DATE_HOUR_NUMBER_FORMAT);
		String urlMD5 = MD5Util.MD5(url);

		// updateTime
		String updateTime = DateFormatUtils.format(new Date(), StringFormatConsts.SIMPLE_DATETIME_FORMAT);

		String content = contentBuff.toString();
		// content.2
		content = content.split(HtmlContentConsts.CONTENT_END)[0];
		// content.3
		content = content.replaceAll(HtmlContentConsts.CONTENT_INNER_MEDIA, "");
		// content.4
		content = content.replaceAll(HtmlRegexConsts.EXHANGE_TAG, "");

		logger.info("result:Start==================<{}>===================", url);
		logger.info(" url:\t\t {}", url);
		logger.info(" publishTime:\t\t {}", publishTime);
		logger.info(" title:\t\t {}", title);
		logger.info(" author:\t\t {}", author);
		logger.info(" groupId:\t\t {}", groupId);
		logger.info(" urlMD5:\t\t {}", urlMD5);
		logger.info(" updateTime:\t\t {}", updateTime);
		logger.info(" content:\t\t {}", content.length() > 64 ? content.substring(0, 64) + "......" : content);
		logger.info("result:End==================================content.length={}", content.length());

		// 装箱
		browserSearchDoc.setAuthor(author);
		browserSearchDoc.setContent(StringUtils.isBlank(content) ? title : content);
		try {
			Date publishDate = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(publishTime);
			publishTime = DateFormatUtils.format(publishDate, "yyyyMMddHH");
		} catch (ParseException e) {
			publishTime = DateFormatUtils.format(new Date(), "yyyyMMddHH");
		}
		browserSearchDoc.setDate(Long.valueOf(publishTime));
		browserSearchDoc.setGroupId(groupId);
		browserSearchDoc.setId(urlMD5);
		browserSearchDoc.setTitle(title);
		browserSearchDoc.setUrl(url);
		return browserSearchDoc;
	}

	
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
			
			if (StringUtils.isBlank(message.getContent())){
				WebDriver driver = null;
				try {
					driver = BrowserFactory.createWindowsPhantomJS();
					driver.get(message.getUrl());
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
				doc = defaultNewsDocBuild(message.getUrl(), message.getContent());
				break;
			case topicDoc:
				break;
			case weixinGzhDoc:
				doc = browserSearchDocBuild(message.getUrl(), message.getContent());
				break;
			default:
				break;
			}
		} else {
			logger.error("message content or buildDocType is null ....");
		}

		return doc;
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

	}

}

class ContentValue {

	public int pCount;
	public String htmlContent;

}
