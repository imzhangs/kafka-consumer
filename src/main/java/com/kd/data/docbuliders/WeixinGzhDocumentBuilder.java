package com.kd.data.docbuliders;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kd.browersearch.domain.BrowserSearchDoc;
import com.kd.commons.consts.HtmlContentConsts;
import com.kd.commons.consts.HtmlRegexConsts;
import com.kd.commons.consts.StringFormatConsts;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.utils.MD5Util;

public class WeixinGzhDocumentBuilder {
	static Logger logger = LoggerFactory.getLogger(WeixinGzhDocumentBuilder.class);

	/**
	 * 微信公众号 文档实体
	 * 
	 * @param url
	 * @param htmlSources
	 * @return
	 */
	public static BrowserSearchDoc browserWeixinGzhDocBuild(KafkaMessage message, boolean isPlainText) {
		

		String url =message.getUrl() ;
		String htmlSources=message.getContent();
		
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
		String publishDate = "";
		String author = "";

		try {
			publishDate = doc.getElementById("post-date").text();
			author = doc.getElementById("post-user").text();
		} catch (Throwable e) {
		}

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
				value.htmlContent = isPlainText ? paperSub.parent().text() : paperSub.parent().html();
				value.htmlContent = value.htmlContent.trim();
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
		String domain = url.replaceFirst(HtmlRegexConsts.DOMAIN, "$2");

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
		logger.info(" publishTime:\t\t {}", publishDate);
		logger.info(" title:\t\t {}", title);
		logger.info(" author:\t\t {}", author);
		logger.info(" groupId:\t\t {}", groupId);
		logger.info(" urlMD5:\t\t {}", urlMD5);
		logger.info(" updateTime:\t\t {}", updateTime);
		logger.info(" content:\t\t {}", content.length() > 64 ? content.substring(0, 64) + "......" : content);
		logger.info("result:End==================================content.length={}", content.length());

		if(StringUtils.isBlank(title) && StringUtils.isBlank(content)){
			return null;
		}
		
		// 装箱
		browserSearchDoc.setAuthor(author);
		browserSearchDoc.setAuthorId(author.hashCode());
		browserSearchDoc.setDomain(domain);
		browserSearchDoc.setContent(StringUtils.isBlank(content) ? title : content);
		browserSearchDoc.setPublishDate(publishDate);
		browserSearchDoc.setDate(Long.valueOf(DateFormatUtils.format(new Date(), StringFormatConsts.DATE_HOUR_NUMBER_FORMAT)));
		browserSearchDoc.setGroupId(groupId);
		browserSearchDoc.setId(urlMD5);
		browserSearchDoc.setTitle(title);
		browserSearchDoc.setUrl(url);
		browserSearchDoc.setSource(message.getSourceId());
		browserSearchDoc.setAuthorAccount(message.getAuthor());
		browserSearchDoc.setType(message.getTypeId());
		
		return browserSearchDoc;
	}

}
