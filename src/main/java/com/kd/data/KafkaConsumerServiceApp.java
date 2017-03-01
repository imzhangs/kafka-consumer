package com.kd.data;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

import com.kd.data.consumer.NativeConsumer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


@SpringBootApplication
@EnableRedisRepositories
public class KafkaConsumerServiceApp {
	

	@Value("${consume.thread.count}")
	int consumeAnalyseThreads;
	
	@Value("${redis.hostname}")
	String redisHostName;
	
	@Value("${redis.port}")
	int redisPort;
	
	@Value("${redis.password}")
	String redisPassword;
	
	@Autowired
	JedisPoolConfig jedisPoolConfig;
	
	@Bean
	public JedisPoolConfig getJedisPoolConfig(){
		JedisPoolConfig jedisPoolConfig= new JedisPoolConfig();
		jedisPoolConfig.setMinIdle(1);
		jedisPoolConfig.setMaxIdle(5);
		jedisPoolConfig.setMaxTotal(30);
		jedisPoolConfig.setMaxWaitMillis(1000);
		jedisPoolConfig.setTestOnBorrow(true);
		jedisPoolConfig.setTestOnReturn(true);
		return jedisPoolConfig;
	}
	
	@Bean
	@SuppressWarnings("resource")
	public Jedis getJedisClient(){
		JedisPool pool=new JedisPool(jedisPoolConfig,redisHostName,redisPort,15000,redisPassword);
		return pool.getResource();
		
	}
	
	@Bean
	public ExecutorService createExcutor(){
		ExecutorService executor = Executors.newFixedThreadPool(consumeAnalyseThreads);
		return executor;
	}

	
	public static void main(String[] args) throws Exception {
		final ApplicationContext applicationContext = SpringApplication.run(KafkaConsumerServiceApp.class, args);
		NativeConsumer consumer=applicationContext.getBean(NativeConsumer.class);
		consumer.init();
		Thread.sleep(3000);
		consumer.consume();
	}
	
}



