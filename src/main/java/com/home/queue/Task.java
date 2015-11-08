package com.home.queue;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class Task {
	private final static String QUEUE_NAME = "task_queue";

	private static String getMessage(String[] strings){
		if (strings.length < 1)
			return "Hello World! ".concat(String.valueOf(Math.random()));
		return String.join(" ", strings);
	}

	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		try {
			String message = getMessage(args);
			channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			System.out.println(" [x] Sent '" + message + "'");
		} finally {
			channel.close();
			connection.close();
		}
	}
}
