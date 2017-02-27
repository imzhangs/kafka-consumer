package com.kd.data.runnable;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.kd.commons.domain.KafkaMessage;
import com.kd.commons.http.HttpRequestUtil;
import com.kd.data.docbuliders.DocumentBuilder;
import com.kd.data.docbuliders.SendMQBuilder;
import com.kd.news.domain.NewsDoc;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerRunable<K, V> implements Runnable {

	static Logger log = LoggerFactory.getLogger(ConsumerRunable.class);

	String contentKeyRegexs;

	String indexSaveUrl;
	
	SendMQBuilder sendMQBuilder;

	public String getContentKeyRegexs() {
		return contentKeyRegexs;
	}

	public void setContentKeyRegexs(String contentKeyRegexs) {
		this.contentKeyRegexs = contentKeyRegexs;
	}

	public String getIndexSaveUrl() {
		return indexSaveUrl;
	}

	public void setIndexSaveUrl(String indexSaveUrl) {
		this.indexSaveUrl = indexSaveUrl;
	}

	Logger logger = LoggerFactory.getLogger(this.getClass());

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
			logger.info("consume >> : " + jsonStr != null && jsonStr.length() > 128 ? jsonStr.substring(0, 128) + "..."
					: jsonStr);
			KafkaMessage message = JSONObject.parseObject(jsonStr, KafkaMessage.class);
			try {
				if (message == null || message.getContent().contains("typeerror: null is not an object")) {
					logger.error("explain failed, message typeerror !!!!");
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

		NewsDoc newsDoc = DocumentBuilder.defaultNewsDocBuild(message.getUrl(), message.getContent());
		if (message.isSaveToIndex()) {
			HttpRequestUtil.postJSON(indexSaveUrl, JSONObject.toJSONString(newsDoc));
		}
		return newsDoc;
	}

	/**
	 * explain
	 * 
	 * @param message
	 */
	public void explain(KafkaMessage message) {

	}

	public void explainTempFileAndSave(KafkaMessage message) {
		if (message == null || StringUtils.isBlank(message.getTempFilePath())) {
			log.error("kafka consumer received message is null...");
			return ;
		}
		String filepath=message.getTempFilePath();
		File tempFile = new File(filepath);
		if (!tempFile.exists()) {
			log.error("tempFile does not exists !!! path ={}",filepath);
			return ;
		}

		// 后期考虑 DFS
		try {
			String content = FileUtils.readFileToString(new File(filepath), "utf-8");
			if(StringUtils.isBlank(content)){
				log.warn("tempFile conetnt is empty ... with path={}",filepath);
			}
			switch(message.getType()){
			case _default:
			case _buildDocument:
				////文档解析	
				{
					Object saveDoc=null;
					switch(message.getBuildDocType()){
					case newsDoc:
						saveDoc = DocumentBuilder.browserSearchDocBuild(message.getUrl(), content);
						break;
					case topicDoc:
						saveDoc = DocumentBuilder.defaultNewsDocBuild(message.getUrl(), content);
						break;
					case weixinGzhDoc:
						break;
					default:
						break;
					}
					String saveResult=HttpRequestUtil.postJSON(indexSaveUrl, JSONObject.toJSONString(saveDoc));
					log.debug("saved successfully ! sources =>>{}",saveResult);
				}
				break;
			case _requestURL:
				message=DocumentBuilder.buildSource(message);
				sendMQBuilder.sendMessgae(message);
				break;
			case _tempSegements:
				//TODO HDFS
				String htmlSources=FileUtils.readFileToString(new File(message.getTempFilePath()),"utf-8");
				sendMQBuilder.urlExplainAndSend(htmlSources);
				break;
			case _customer:
				break;
			default:
				break;
			
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	
}
