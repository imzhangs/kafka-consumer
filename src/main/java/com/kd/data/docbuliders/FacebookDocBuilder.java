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
	public static  List<FacebookDoc> tempFacebookTopicBuild(KafkaMessage message, boolean isPlainText) {
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
			
			Elements dataList = htmlDoc.select("div[class=_427x]");
			if(dataList==null || dataList.isEmpty()){
				dataList=htmlDoc.select("ol[id=u_0_16_story]>div[class=_5pcb _4b0l]>div[class=_4-u2 mbm _4mrt _5jmm _5pat _5v3q _4-u8]");
			}
			for (Element node : dataList) {
				author = node.select("span[class=fwb fcg]").text();
				
				publishDate = node.select("abbr[class=_5ptz]").attr("title");
				publishDate = DocumentBuilder.getPublishTime(publishDate);
				
				subUrl= node.select("span[class=fsm fwn fcg]>a[class=_5pcq").attr("href");
				subUrl=subUrl.startsWith("http")?subUrl:"http://www.facebook.com"+subUrl;
				fbId = MD5Util.MD5(subUrl+publishDate);
						
				contentText = node.select("div[class=_5pbx userContent]>div>p").text();
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
				facebookDoc.setAuthorId(authorId);
				facebookDoc.setClickNum(like);
				facebookDoc.setCommentNum(comment);
				facebookDoc.setForwardNum(forward);
				facebookDoc.setPublishDate(publishDate);
				facebookDoc.setContent(contentText);
				facebookDoc.setComments(commentText);
				facebookDoc.setDocType(BuildDocTypeEnum.facebookDoc);
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
