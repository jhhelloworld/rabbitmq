package com.routing;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * 功能：只订阅全部消息的子集，例如只把错误信息存储在日志文件中，同时仍然能够在控制台上打印所有的日志信息
 * 实现方式：试用 “direct”类型的交换机，message只进入特定的队列：  队列的 binding key 和 message的 routing key 必须相等
 *
 *
 */
public class ReceiveLogsDirect {

  private static final String EXCHANGE_NAME = "direct_logs";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
    String queueName = channel.queueDeclare().getQueue();

    if (argv.length < 1){
      System.err.println("Usage: com.routing.ReceiveLogsDirect [info] [warning] [error]");
      System.exit(1);
    }
    for(String severity : argv){
      channel.queueBind(queueName, EXCHANGE_NAME, severity);
    }
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
      }
    };
    channel.basicConsume(queueName, true, consumer);
  }
}

