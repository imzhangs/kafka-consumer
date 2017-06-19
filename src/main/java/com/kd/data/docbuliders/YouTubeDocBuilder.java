package com.kd.data.docbuliders;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.kd.browersearch.domain.FacebookDoc;
import com.kd.commons.consts.StringFormatConsts;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.enums.BuildDocTypeEnum;
import com.kd.commons.utils.MD5Util;

@Component
@ConfigurationProperties(value="youtube.properties")
public class YouTubeDocBuilder {
	
	static Logger logger = LoggerFactory.getLogger(YouTubeDocBuilder.class);
	
	@Value("${youtube.context.list}")
	String contentListPath;

	@Value("${youtube.context.author}")
	String authorPath;

	@Value("${youtube.context.title}")
	String titlePath;

	@Value("${youtube.context.contentText}")
	String contentPath;

	@Value("${youtube.context.publishDate}")
	String publishDatePath;

	@Value("${youtube.context.subUrl}")
	String subUrlPath;

	@Value("${youtube.context.attentions}")
	String attentionsPath  ;

	@Value("${youtube.context.like}")
	String likePath; 
	

	@SuppressWarnings("finally")
	public   List<FacebookDoc> tempFacebookTopicBuild(KafkaMessage message, boolean isPlainText) {
		List<FacebookDoc> facebookList = new ArrayList<>();
		if(message==null || StringUtils.isBlank(message.getContent())){
			logger.error("message content is null !!!!");
			return facebookList;
		}
		
		try {
			Document htmlDoc=Jsoup.parse(message.getContent());
			String author = "";
			String fbId = "";
			String publishDate = "";
			String contentText = "";
			int forward = 0;
			int comment = 0;
			int like = 0;
			String subUrl = "";
			int authorId=0;
			String attentions = "0";
			String fans = "0";
			String signMind = "0";
			String title="";
			
			Elements dataList = htmlDoc.select(contentListPath);
			if(dataList==null || dataList.isEmpty()){
				logger.error("not found context data lists !!!");
				return facebookList;
			}
			
			for (Element node : dataList) {
				author = node.select(authorPath).text();
				
				publishDate = node.select("abbr[class=_5ptz]").attr("title");
				publishDate = DocumentBuilder.getPublishTime(publishDate);
				
				subUrl= node.select("span[class=fsm fwn fcg]>a[class=_5pcq").attr("href");
				subUrl=subUrl.startsWith("http")?subUrl:"https://www.youtube.com"+subUrl;
				fbId = MD5Util.MD5(subUrl);
				
				title=node.select(titlePath).text();
				contentText = node.select(contentPath).text();
				
				title=StringUtils.isBlank(title)?contentText:title;
				contentText=StringUtils.isBlank(contentText)?title:contentText;
				
				attentions = node.select(attentionsPath).text();
				try {
					attentions = attentions.replaceAll(".*(\\d+[.,]*[\\d]*)([万]?).*", "$1");
					like = Double.valueOf(attentions).intValue();
				} catch (Throwable e) {
				}
				
				try {
					authorId=Integer.valueOf(author.hashCode());
				} catch (Throwable e) {
				}
				
				FacebookDoc facebookDoc = new FacebookDoc();
				
				try {
					facebookDoc.setAttentionsCount(Integer.valueOf(attentions));
				} catch (Throwable e) {
				}
				
				Date publishedDate=new Date();
				try {
					facebookDoc.setFansCount(Integer.valueOf(fans));
					publishedDate=DateUtils.parseDate(publishDate, DocumentBuilder.shortTimeFormat);
				} catch (Throwable e) {
				}
				
				facebookDoc.setId(fbId);
				facebookDoc.setAuthor(author);
				facebookDoc.setTitle(title);
				facebookDoc.setAuthorId(authorId);
				facebookDoc.setClickNum(like);
				facebookDoc.setCommentNum(comment);
				facebookDoc.setForwardNum(forward);
				facebookDoc.setPublishDate(publishDate);
				facebookDoc.setContent(contentText);
				facebookDoc.setDocType(BuildDocTypeEnum.youtobeDoc);
				facebookDoc.setGroupId(DateFormatUtils.format(publishedDate, StringFormatConsts.DATE_NUMBER_FORMAT));
				facebookDoc.setDate(publishedDate.getTime());
				facebookDoc.setSignMind(signMind);
				facebookDoc.setUrl(subUrl);
				facebookDoc.setSource(message.getSourceId());
				facebookDoc.setLevel(message.getLevel());
				facebookDoc.setType(message.getTypeId());
				
				facebookList.add(facebookDoc);
			}
			
		} catch (Throwable e) {
			// logger.error(e.getCause().toString());
			e.printStackTrace();
		} finally {
			return facebookList;
		}
	}

}
