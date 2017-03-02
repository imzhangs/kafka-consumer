package com.kd.data.runnable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.http.HttpRequestUtil;
import com.kd.data.docbuliders.DocumentBuilder;
import com.kd.data.docbuliders.NewsDocumentBuilder;
import com.kd.data.docbuliders.SendMQBuilder;
import com.kd.news.domain.NewsDoc;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerRunable<K, V> implements Runnable {


	static Logger log = LoggerFactory.getLogger(ConsumerRunable.class);

	String contentKeyRegexs;

	String weixinGzhIndexSaveUrl;
	
	String weiboIndexSaveUrl;
	
	SendMQBuilder sendMQBuilder;
	
	String dbSaveUrl;
	
	
	

	public String getWeixinGzhIndexSaveUrl() {
		return weixinGzhIndexSaveUrl;
	}

	public void setWeixinGzhIndexSaveUrl(String weixinGzhIndexSaveUrl) {
		this.weixinGzhIndexSaveUrl = weixinGzhIndexSaveUrl;
	}

	public String getWeiboIndexSaveUrl() {
		return weiboIndexSaveUrl;
	}

	public void setWeiboIndexSaveUrl(String weiboIndexSaveUrl) {
		this.weiboIndexSaveUrl = weiboIndexSaveUrl;
	}

	public String getDbSaveUrl() {
		return dbSaveUrl;
	}

	public void setDbSaveUrl(String dbSaveUrl) {
		this.dbSaveUrl = dbSaveUrl;
	}

	public SendMQBuilder getSendMQBuilder() {
		return sendMQBuilder;
	}

	public void setSendMQBuilder(SendMQBuilder sendMQBuilder) {
		this.sendMQBuilder = sendMQBuilder;
	}

	public String getContentKeyRegexs() {
		return contentKeyRegexs;
	}

	public void setContentKeyRegexs(String contentKeyRegexs) {
		this.contentKeyRegexs = contentKeyRegexs;
	}

	private KafkaStream<K, V> dataStream;

	public ConsumerRunable() {
	}

	public KafkaStream<K, V> getDataStream() {
		return dataStream;
	}

	public void setDataStream(KafkaStream<K, V> dataStream) {
		this.dataStream = dataStream;
	}

	public ConsumerRunable(KafkaStream<K, V> dataStream) {
		this.dataStream = dataStream;
	}

	public void run() {
		ConsumerIterator<K, V> it = dataStream.iterator();
		String jsonStr = "{}";
		while (it.hasNext()) {
			jsonStr = new String((byte[]) it.next().message());
			log.info("consume >> : " + jsonStr != null && jsonStr.length() > 128 ? jsonStr.substring(0, 128) + "..."
					: jsonStr);
			KafkaMessage message = JSONObject.parseObject(jsonStr, KafkaMessage.class);
			try {
				if (message == null  ) {
					log.error("explain failed, message url error !!!!");
					continue;
				}
				
				explain(message);
				
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 消息内容解析
	 * 
	 * @param message
	 * @return
	 */
	public NewsDoc explainContent(KafkaMessage message) {

		if (null == message || StringUtils.isBlank(message.getContent())) {
			return null;
		}
		
		NewsDoc newsDoc = NewsDocumentBuilder.defaultNewsDocBuild(message.getUrl(), message.getContent());
		if (message.isSaveToIndex()) {
			HttpRequestUtil.postJSON(weixinGzhIndexSaveUrl, JSONObject.toJSONString(newsDoc));
		}
		return newsDoc;
	}

	/**
	 * explain
	 * 
	 * @param message
	 */

	public void explain(KafkaMessage message) {
	
		try {
			switch(message.getType()){
			case _default:
			case _buildDocument:
				////文档解析	
				if(null==message.getBuildDocType()){
					log.error(" message.getBuildDocType() is null !!!");
					break;
				}
				DocumentBuilder.weixinSaveIndex=weixinGzhIndexSaveUrl;
				DocumentBuilder.weixinSaveDB=dbSaveUrl;
				DocumentBuilder.weiboSaveIndex=weiboIndexSaveUrl;
				DocumentBuilder.weiboSaveDB=dbSaveUrl;
				
				DocumentBuilder.docBuilderAndSave(message);
				break;
			case _requestURL:
				message=DocumentBuilder.buildSource(message);
				sendMQBuilder.sendMessgae(message);
				break;
			case _tempSegements:
				//TODO 	// 后期考虑 DFS
				String filepath=message.getTempFilePath();
				File tempFile = new File(filepath);
				if (!tempFile.exists()) {
					log.error("tempFile does not exists !!! path ={}",filepath);
					return ;
				}
				String content = FileUtils.readFileToString(new File(filepath), "utf-8");
				if(StringUtils.isBlank(content)){
					log.warn("tempFile conetnt is empty ... with path={}",filepath);
				}else{
					sendMQBuilder.urlExplainAndSend(content,message.getBuildDocType());
				}
				break;
			case _customer:
				break;
			default:
				break;
			
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}

	}
	
	
	
}
