package com.publishSubscribe;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

/**
 * 之前：每个任务分给一个Worker，现在：把一条消息发给多个消费者  这种模式为：发布/订阅
 * 构建一个日志系统，生产者发送消息，每个消费者都能接收消息，其中一个把日志引导到磁盘，同时运行另一个消费者把日志输出到屏幕
 * 本质上，日志消息会向所有的消费者广播
 * RabbitMQ消息传递模型的核心思想：生产者不会向队列直接发送消息，也不会知道消息是否会被传送到队列
 *生产者只能把消息发送给交换机（exchange）交换机从生产者接收消息并把消息发送到队列
 * 交换机需要知道如何处理接收到的消息：发送到一个/多个队列，是否丢弃等，这些规则由交换机的type决定： direct, topic, headers fanout
 */
public class EmitLog {

    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            //声明交换机类型  fanout类型的交换机 把接收到的所有消息广播到他知道的所有队列中
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            String message = getMessage(argv);

            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }

    private static String getMessage(String[] strings) {
        if (strings.length < 1){
            return "info: Hello World!";
        }
        return String.join(" ", strings);
    }

}

