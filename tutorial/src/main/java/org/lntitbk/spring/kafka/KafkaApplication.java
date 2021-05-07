package org.lntitbk.spring.kafka;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaApplication {
	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);
		MessageProducer producer = context.getBean(MessageProducer.class);
		MessageListener listener = context.getBean(MessageListener.class);
		
		producer.sendMessage("Hello, world!");
		listener.getLatch().await(10, TimeUnit.SECONDS);
		
		for(int i = 0; i < 5; i++) {
			producer.sendMessageToPartition("Hello To Partitioned Topic!", i);
		}
		
		listener.getPartitionLatch().await(10, TimeUnit.SECONDS);
		
		producer.sendMessageToFiltered("Hello Trinh!!");
		producer.sendMessageToFiltered("Hello World!");
		listener.getFilterLatch().await(10, TimeUnit.SECONDS);
		
		producer.sendGreetingMessage(new Greeting("Greetings", "World"));
		listener.getGreetingLacth().await(10, TimeUnit.SECONDS);
		context.close();
	}
	
	@Bean
	public MessageProducer messageProducer() {
		return new MessageProducer();
	}
	
	@Bean
	public MessageListener messageListener() {
		return new MessageListener();
	}
}
