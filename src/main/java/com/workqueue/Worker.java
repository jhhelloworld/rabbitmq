package com.workqueue;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * 运行一个newTask,多个Worker
 * 消息确认机制： 消费者向RabbitMQ 发送 ack，表示消息被接收处理，RabbitMQ可以删除该消息，
 * 如果消费者死亡（通道、连接关闭、TCP连接丢失，消费者向RabbitMQ把消息重新排队，如果有其他消费者，则托付给其他消费者）
 * 没有超时，当消费者死亡时RabbitMQ重传这些消息
 * 注意：消息持久性不能保证信息不会丢失，尽管RabbitMQ被通知保存消息，但是RabbitMQ到保存还是有一段时间，并且RabbitMQ不为每条消息执行fsync（2）——它可能被保存到缓存中，而不是真正写入磁盘。
 * 如果需要更强的保证 ：https://www.rabbitmq.com/confirms.html
 */
public class Worker {

  private static final String TASK_QUEUE_NAME = "task_queue";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();

    //消息持久性：设置为TRUE  RabbitMQ 服务停止，消息不丢失，
    // 应同时使消息和队列具有持久性   下面是队列持久性，需要在生产者和消费者同时声明
    //消息持久性 在 消费者 声明
    boolean durable = true;
    channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    /**
     * 默认情况，RabbitMQ依次向下一个消费者发送每一条消息，平均每个消费者得到相同数量的消息
     * 问题：假设两个Worker，所有奇数消息任务很重，会使其中一个Worker一直忙，另一个很闲
     * 之所以发生这种情况，是因为RabbitMQ在消息进入队列时才会发送一条消息。它不关注未被确认的消息的数量。
     * 它只是盲目地将每一个n条信息发送给第n个消费者。
     * 解决方法：prefetchCount = 1;  RabbitMQ不会一次给一个工人发送多个消息。换句话说，在处理并确认前一个消息之前，不会向Worker发送新消息，会把它分派给下一个不太忙的Worker。
     */
    int prefetchCount = 1;
    channel.basicQos(prefetchCount);

    final Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");

        System.out.println(" [x] Received '" + message + "'");
        try {
          doWork(message);
        } finally {
          System.out.println(" [x] Done");
          channel.basicAck(envelope.getDeliveryTag(), false);
        }
      }
    };


    //autoAck:true   RabbitMQ每发出一条消息认为消息被确认（acknowledged ）
    //autoAck:false  使用消息确认机制
    boolean autoAck=false;
    channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);
  }

  /**
   * 模拟复杂任务，字符串的每个“.”代表一秒钟，模拟执行任务时间
   * @param task
   */
  private static void doWork(String task) {
    for (char ch : task.toCharArray()) {
      if (ch == '.') {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException _ignored) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}

