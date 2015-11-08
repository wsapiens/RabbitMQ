package com.home.pubsub.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogTopic {
	private static final String EXCHANGE_NAME = "topic_logs";

	public static void main(String[] argv) throws Exception {

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.exchangeDeclare(EXCHANGE_NAME, "topic");

		//<speed>.<colour>.<species>
		String speed = "lazy";
		if( new Double(Math.random() * 1000000.0d).intValue() % 2 == 1) {
			speed = "quick";
		}

		String colour = "white";
		switch(new Double(Math.random() * 1000000.0d).intValue() % 5) {
		case 1: colour = "yellow";
			break;
		case 2: colour = "orange";
			break;
		case 3: colour = "red";
			break;
		case 4: colour = "purple"; 
			break;
		}

		String species = "elephant";
		switch(new Double(Math.random() * 1000000.0d).intValue() % 5) {
		case 1: species = "buffalo";
			break;
		case 2: species = "ram";
			break;
		case 3:	species = "rabbit";
			break;
		case 4: species = "hamster";
			break;
		}
		
		
		
		
		
		
		try{
			String routingKey = String.join(".", speed,colour,species);
			String message = String.join("", "[", routingKey, "] message");

			channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
			System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
		} finally {
			channel.close();
			connection.close();
		}
	}
}
