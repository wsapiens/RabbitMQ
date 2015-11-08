package com.home.pubsub.direct;

import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogDirect {
	private static final String EXCHANGE_NAME = "direct_logs";

	public static void main(String[] argv) throws java.io.IOException, TimeoutException {

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.exchangeDeclare(EXCHANGE_NAME, "direct");
		int level = new Double(Math.random() * 1000000.0d).intValue() % 3;

		String severity = "info";
		switch(level) {
		case 1:	severity = "warning";
			break;
		case 2:	severity = "error";
			break;
		}
		String message = String.join("", "[",severity, "] ", "message");

		try {
			channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());
			System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
		} finally {
			channel.close();
			connection.close();
		}
	}
}
