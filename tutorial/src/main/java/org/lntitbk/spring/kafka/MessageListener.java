package org.lntitbk.spring.kafka;

import java.util.concurrent.CountDownLatch;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
//import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

public class MessageListener {
	private CountDownLatch latch = new CountDownLatch(2);
	private CountDownLatch partitionLatch = new CountDownLatch(2);
	private CountDownLatch filterLatch = new CountDownLatch(2);
	private CountDownLatch greetingLacth = new CountDownLatch(2);
	
	@KafkaListener(topics = "${message.topic.name}", groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
	public  void listenGroupFoo(String message) {
		System.out.println("Received Message in Group 'foo' : " + message);
		latch.countDown();
	}
	
	@KafkaListener(topics = "${message.topic.name}", groupId = "bar", containerFactory = "barKafkaListenerContainerFactory")
	public  void listenGroupBar(String message) {
		System.out.println("Received Message in Group 'foo' : " + message);
		latch.countDown();
	}
	
	@KafkaListener(topics = "${message.topic.name}", containerFactory = "headersKafkaListenerContainerFactory")	
	public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) int partition) {
		System.out.println("Received Message : " + message + " from partition :" + partition);
		latch.countDown();
	}
	
	@KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = {"0", "3"}), containerFactory = "partitionsKafkaListenerContainerFactory")
	public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println("Received Message:" + message + " from patition: " + partition);
		this.partitionLatch.countDown();
	}
	 
	@KafkaListener(topics = "${filtered.topic.name}", containerFactory = "filterKafkaListenerContainerFactory")
	public void listenWithFilter(String message) {
		System.out.println("Received Message in filtered listener: " + message);
		this.filterLatch.countDown();
	}
	 
	@KafkaListener(topics = "${greeting.topic.name}", containerFactory = "greetingKafkaListenerContainerFactory")
	public void greetingListener(Greeting greeting) {
			System.out.println("Received greeting message: " + greeting);
			this.greetingLacth.countDown();
	}
	 
	@KafkaListener(topics = "${greeting.topic.name}", containerFactory = "greetingKafkaListenerContainerFactory")
	public CountDownLatch getLatch() {
		return latch;
	}

	public CountDownLatch getPartitionLatch() {
		return partitionLatch;
	}

	public CountDownLatch getFilterLatch() {
		return filterLatch;
	}

	public CountDownLatch getGreetingLacth() {
		return greetingLacth;
	}
	
}