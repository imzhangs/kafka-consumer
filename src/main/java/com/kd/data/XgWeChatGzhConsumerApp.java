package com.kd.data;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

import com.kd.data.consumer.NativeConsumer;


@SpringBootApplication
@EnableRedisRepositories
public class XgWeChatGzhConsumerApp {

	@Value("${consume.thread.count}")
	int consumeAnalyseThreads;

	@Bean
	public ExecutorService createExcutor(){
		ExecutorService executor = Executors.newFixedThreadPool(consumeAnalyseThreads);
		return executor;
	}

	
	public static void main(String[] args) throws Exception {
		final ApplicationContext applicationContext = SpringApplication.run(XgWeChatGzhConsumerApp.class, args);
		NativeConsumer consumer=applicationContext.getBean(NativeConsumer.class);
		consumer.init();
		consumer.topic="xiguaji-topic";
		Thread.sleep(3000);
		consumer.consume();
	}
	
}



