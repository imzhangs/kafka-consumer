package com.kd.data.runnable;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DateFormatUtils;
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

	String indexSaveUrl;
	
	SendMQBuilder sendMQBuilder;
	

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
				if (message == null  ) {
					logger.error("explain failed, message url error !!!!");
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
	
		try {
			switch(message.getType()){
			case _default:
			case _buildDocument:
				////文档解析	
				if(null==message.getBuildDocType()){
					log.error(" message.getBuildDocType() is null !!!");
					break;
				}
				String saveDoc=DocumentBuilder.docBuilderString(message);
				if(StringUtils.isBlank(saveDoc)){
					log.error(" saveDoc body  is null !!!");
					break;
				}
				String saveResult=HttpRequestUtil.postJSON(indexSaveUrl, saveDoc);
				String date=DateFormatUtils.format(new Date(), "yyyyMMddHH");
				FileUtils.writeStringToFile(new File("/data/logs/"+date+"/saveResult.log"),saveDoc+"\n", "utf-8", true);
				log.info("======================>>>saved successfully ! sources =>>{}",saveResult);
				
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
