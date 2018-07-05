package com.publishSubscribe;

import com.rabbitmq.client.*;

import java.io.IOException;

public class ReceiveLogs {
  private static final String EXCHANGE_NAME = "logs";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

    //暂时的队列
    //对于日志系统来说，希望收到所有的消息，并且只对当前消息流感兴趣，因此：
    //当连接到Rabbit 时需要新的空队列，Rabbit为队列取随机的名字
    //当消费者断开连接时，低于队列自动被删除
    //queueDeclare()不输入任何参数，建立一个非持久的，排他的，自动删除并且随机生成名字的队列
    //其他队列详细介绍 http://www.rabbitmq.com/tutorials/tutorial-three-java.html
    String queueName = channel.queueDeclare().getQueue();
    //绑定消息交换机和队列
    channel.queueBind(queueName, EXCHANGE_NAME, "");

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + message + "'");
      }
    };
    channel.basicConsume(queueName, true, consumer);
  }
}

