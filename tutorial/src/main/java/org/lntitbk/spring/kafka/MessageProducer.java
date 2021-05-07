package org.lntitbk.spring.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class MessageProducer {
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	  @Value(value = "${message.topic.name}")
      private String topicName;

      @Value(value = "${partitioned.topic.name}")
      private String partitionedTopicName;

      @Value(value = "${filtered.topic.name}")
      private String filteredTopicName;

      @Value(value = "${greeting.topic.name}")
      private String greetingTopicName;
      
      public void sendMessage(String msg) {
    	  ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, msg);
    	  future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				// TODO Auto-generated method stub
				System.out.println("Sent message=[" + msg + "] with offset=[" + result.getRecordMetadata().offset() + "]");
			}

			@Override
			public void onFailure(Throwable ex) {
				// TODO Auto-generated method stub
				System.out.println("Unable to send message=[" 
			              + msg + "] due to : " + ex.getMessage());
			}
		});
      }
      
      public void sendMessageToPartition(String message, int partition) {
    	  kafkaTemplate.send(partitionedTopicName, partition, null, message);
      }
      
      public void sendMessageToFiltered(String message) {
    	  kafkaTemplate.send(filteredTopicName, message);
      }
      
      public void sendGreetingMessage(Greeting greeting) {
    	  
      }
}
