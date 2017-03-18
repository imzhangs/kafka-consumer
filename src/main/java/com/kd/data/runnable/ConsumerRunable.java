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
import com.kd.data.docbuliders.NewsDocumentBuilder;
import com.kd.data.docbuliders.SendMQBuilder;
import com.kd.news.domain.NewsDoc;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerRunable<K, V> implements Runnable {

	static Logger log = LoggerFactory.getLogger(ConsumerRunable.class);

	String contentKeyRegexs;

	SendMQBuilder sendMQBuilder;

	public String getPhantomJSPath() {
		return DocumentBuilder.phantomJSPath;
	}

	public void setPhantomJSPath(String phantomJSPath) {
		DocumentBuilder.phantomJSPath = phantomJSPath;
	}

	public String getWindowsPhantomJSPath() {
		return DocumentBuilder.windowsPhantomJSPath;
	}

	public void setWindowsPhantomJSPath(String windowsPhantomJSPath) {
		DocumentBuilder.windowsPhantomJSPath = windowsPhantomJSPath;
	}

	public String getWeiboSaveDBUrl() {
		return DocumentBuilder.weiboSaveDB;
	}

	public void setWeiboSaveDBUrl(String weiboSaveDBUrl) {
		DocumentBuilder.weiboSaveDB = weiboSaveDBUrl;
	}

	public String getWeixinGzhIndexSaveUrl() {
		return DocumentBuilder.weixinSaveIndex;
	}

	public void setWeixinGzhIndexSaveUrl(String weixinGzhIndexSaveUrl) {
		DocumentBuilder.weixinSaveIndex = weixinGzhIndexSaveUrl;
	}

	public String getWeiboIndexSaveUrl() {
		return DocumentBuilder.weiboSaveIndex;
	}

	public void setWeiboIndexSaveUrl(String weiboIndexSaveUrl) {
		DocumentBuilder.weiboSaveIndex = weiboIndexSaveUrl;
	}

	public String getDbSaveUrl() {
		return DocumentBuilder.weixinSaveDB;
	}

	public void setDbSaveUrl(String dbSaveUrl) {
		DocumentBuilder.weixinSaveDB = dbSaveUrl;
	}

	public String getRemoteDicSplitUrl() {
		return DocumentBuilder.remoteDicSplitUrl;
	}

	public void setRemoteDicSplitUrl(String remoteDicSplitUrl) {
		DocumentBuilder.remoteDicSplitUrl = remoteDicSplitUrl;
	}

	public void setRemoteDicTopic(String topic) {
		DocumentBuilder.remoteDicTopic = topic;
	}

	public String getRemoteDicTopic() {
		return DocumentBuilder.remoteDicTopic;
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
				if (message == null) {
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
			HttpRequestUtil.postJSON(getWeixinGzhIndexSaveUrl(), JSONObject.toJSONString(newsDoc));
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
			switch (message.getType()) {
			case _default:
			case _buildDocument:
				//// 文档解析
				if (null == message.getBuildDocType()) {
					log.error(" message.getBuildDocType() is null !!!");
					break;
				}
				DocumentBuilder.docBuilderAndSave(message);
				break;
			case _requestURL:
				message.setUrl(
						message.getUrl().startsWith("http://") ? message.getUrl() : "http://" + message.getUrl());
				message = DocumentBuilder.buildSource(message);
				sendMQBuilder.sendMessgae(message);
				break;
			case _tempSegements:
				// TODO // 后期考虑 DFS
				String filepath = message.getTempFilePath();
				File tempFile = new File(filepath);
				if (!tempFile.exists()) {
					log.error("tempFile does not exists !!! path ={}", filepath);
					return;
				}
				String content = FileUtils.readFileToString(new File(filepath), "utf-8");
				message.setContent(content);
				if (StringUtils.isBlank(content)) {
					log.warn("tempFile conetnt is empty ... with path={}", filepath);
				} else {
					sendMQBuilder.urlExplainAndSend(message);
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
		} finally {

		}

	}

}
