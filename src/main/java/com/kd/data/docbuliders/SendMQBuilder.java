package com.kd.data.docbuliders;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.kd.commons.consts.HtmlRegexConsts;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.enums.BuildDocTypeEnum;
import com.kd.commons.enums.ExplainTypeEnum;
import com.kd.commons.http.HttpRequestUtil;

@Component
public class SendMQBuilder {

	@Value("${kafka.topic}")
	String topic;
	
	@Value("${producer.send.url}")
	String kfkProducerUrl;
	
	/**
	 * 从html 文档中 抓出所有的 链接 并发到 消息队列
	 * @param htmlSources html document
	 */
	public  void urlExplainAndSend(String htmlSources,BuildDocTypeEnum buildDocType){
		Document doc = Jsoup.parse(htmlSources);
		Elements elements=doc.select("a");
		for(Element it:elements){
			String url=it.attr("href");
			if(StringUtils.isBlank(url) || !url.matches(HtmlRegexConsts.DOMAIN)){
				continue;
			}
			KafkaMessage message=new KafkaMessage();
			message.setUrl(url);
			message.setType(ExplainTypeEnum._requestURL);
			message.setTopic(topic);
			message.setBuildDocType(buildDocType);
			sendMessgae( message);
		}
	}
	
	
	public void sendMessgae(KafkaMessage message){
		HttpRequestUtil.postJSON(kfkProducerUrl, JSONObject.toJSONString(message));
	}
}
