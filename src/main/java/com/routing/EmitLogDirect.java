package com.routing;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *把消息发送到direct类型的交换机，把日志错误的严重程度作为routing key。接收程序可以选择它希望接收到的错误程度的日志
 */
public class EmitLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            //创建交换机
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            String severity = getSeverity(argv);
            String message = getMessage(argv);

            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        }
    }

    private static String getSeverity(String[] strings) {
        if (strings.length < 1){
            return "info";
        }
        return strings[0];
    }

    private static String getMessage(String[] strings) {
        if (strings.length < 2){
            return "Hello World!";
        }
        return String.join(" ",Arrays.copyOfRange(strings, 1, strings.length));
    }

}

