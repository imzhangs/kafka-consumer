package com.kd.data.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.kd.data.runnable.ConsumerRunable;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

@Component
public class NativeConsumer {

	@Value("${zookeeper.server.hosts-ports}")
	String zkHostports;

	@Value("${topic.groupid}")
	String groupId;

	@Value("${kafka.topic}")
	String topic;
	
	@Value("${serializer.encoding}")
	String encoding;
	
	@Value("${content.key.Regexs}")
	String contentKeyRegexs;

	@Value("${index.save.url}")
	String indexSaveUrl;
	

	@Value("${consume.thread.count}")
	int consumeAnalyseThreads;

	@Autowired
	ExecutorService executor;
	
	private ConsumerConnector consumer;
	
	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId,int consumeAnalyseThreads,String encoding) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
//		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("serializer.encoding", encoding);
		props.put("mirror.consumer.numthreads", consumeAnalyseThreads);
		return new ConsumerConfig(props);
	}

	public void init() {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zkHostports, groupId,consumeAnalyseThreads,encoding));
	}

	public void consume() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		for(int i=0;i<consumeAnalyseThreads;i++){
			topicCountMap.put(topic,i );
		}
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		
		// now create an object to consume the messages
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			ConsumerRunable<byte[], byte[]> consumer=	new ConsumerRunable<>(stream);
			consumer.setContentKeyRegexs(contentKeyRegexs);
			consumer.setIndexSaveUrl(indexSaveUrl);
			executor.submit(consumer);
		}
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}
}

