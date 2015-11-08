package com.home.pubsub.fanout;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLog {
	private final static String EXCHANGE_NAME = "logs";

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
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

		try {
			String message = getMessage(args);
			channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
			System.out.println(" [x] Sent '" + message + "'");
		} finally {
			channel.close();
			connection.close();
		}
	}
}
