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

import com.kd.browersearch.domain.FacebookDoc;
import com.kd.commons.consts.StringFormatConsts;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.enums.BuildDocTypeEnum;
import com.kd.commons.utils.MD5Util;

public class FacebookDocBuilder {
	
	static Logger logger = LoggerFactory.getLogger(FacebookDocBuilder.class);
	

	@SuppressWarnings("finally")
	public static List<FacebookDoc> tempFacebookTopicBuild(KafkaMessage message, boolean isPlainText) {
		List<FacebookDoc> facebookList = new ArrayList<>();
		if(message==null || StringUtils.isBlank(message.getContent())){
			logger.error("message content is null !!!!");
			return facebookList;
		}
		
		String url=message.getUrl();
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
			
			Elements cardList = htmlDoc.select("div[class=_427x]");
			for (Element node : cardList) {
				author = node.select("span[class=fwb fcg]").text();
				
				publishDate = node.select("abbr[class=_5ptz]").attr("title");
				publishDate = getFaceBookFullPublishTime(publishDate);
				
				subUrl= node.select("span[class=fsm fwn fcg]>a[class=_5pcq").attr("href");
				subUrl=subUrl.startsWith("http")?subUrl:"http://www.facebook.com"+subUrl;
				fbId = MD5Util.MD5(subUrl+publishDate);
						
				contentText = node.select("div[class=_5pbx userContent]").text();
				Elements commentList=node.select("div[class=UFICommentContentBlock]");
				StringBuilder commentTextBuilder=new StringBuilder();
				for(Element commentEl:commentList){
					String commentAuthor=commentEl.select("span[class= UFICommentActorName]").text();
					String commentBody=commentEl.select("span[class= UFICommentBody]").text();
					commentTextBuilder.append(commentAuthor+":\""+commentBody+"\";;;");
				}
				String commentText =commentTextBuilder.toString();
				
				String likeText = node.select("span[class=UFILikeSentenceText]").text();
				likeText=likeText.replaceAll("[^\\d]*(\\d+).*", "$1");
				if(StringUtils.isBlank(contentText) && StringUtils.isBlank(author) && StringUtils.isBlank(publishDate)){
					continue;
				}
				
				try {
					likeText = likeText.replaceAll(".*(\\d+[.,]*[\\d]*)([万]?).*", "$1");
					like = Double.valueOf(likeText).intValue();
				} catch (Throwable e) {
				}
				
				try {
					String authorIdText=url.substring(url.lastIndexOf("/"),url.length());
					authorId=Integer.valueOf(authorIdText);
				} catch (Throwable e) {
				}
				
				FacebookDoc facebookDoc = new FacebookDoc();
				
				try {
					facebookDoc.setAttentionsCount(Integer.valueOf(attentions));
				} catch (Throwable e) {
				}
				try {
					facebookDoc.setFansCount(Integer.valueOf(fans));
				} catch (Throwable e) {
				}
				
				facebookDoc.setId(fbId);
				facebookDoc.setAuthor(author);
				facebookDoc.setAuthorId(authorId);
				facebookDoc.setClickNum(like);
				facebookDoc.setCommentNum(comment);
				facebookDoc.setForwardNum(forward);
				facebookDoc.setPublishDate(publishDate);
				facebookDoc.setContent(contentText);
				facebookDoc.setComments(commentText);
				facebookDoc.setDocType(BuildDocTypeEnum.facebookDoc);
				facebookDoc.setGroupId(DateFormatUtils.format(new Date(), StringFormatConsts.DATE_NUMBER_FORMAT));
				facebookDoc.setSignMind(signMind);
				facebookDoc.setUrl(subUrl);
				facebookDoc.setDate(System.currentTimeMillis());
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
	
	public static String getFaceBookFullPublishTime(String time) throws Exception {
		String shortTimeFormat="yyyy-MM-dd HH:mm";
		if (StringUtils.isBlank(time)) {
			return ""; 
		}
		time = time.trim();
		if (time.matches("[\\d]{4}-[\\d]{1,2}-[\\d]{1,2}[\\s]+[\\d]{1,2}:[\\d]{1,2}[\\s]*")) {
			return time;
		}
		
		if (time.matches("[\\d]{4}年[\\d]{1,2}月[\\d]{1,2}日[\\s]+[\\d]{1,2}:[\\d]{1,2}[\\s]*")) {
			time= time.replaceAll("[年月]{1}","-").replaceAll("日","");
			return DateFormatUtils.format(DateUtils.parseDate(time, "yyyy-M-d H:m"), shortTimeFormat);
		}
		if (time.matches("^1[4-8][\\d]{8}$")) {
			return DateFormatUtils.format(Long.valueOf(time+"000"), shortTimeFormat);
			
		}
		if (time.matches("^1[4-8][\\d]{11}$")) {
			return DateFormatUtils.format(Long.valueOf(time), shortTimeFormat);
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
		

		return time;
	}
	
}
