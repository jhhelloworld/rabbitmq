package com.helloworld;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.*;

/**
 *rabbitmq : 接收、存储、转发二进制数据
 * 生产者：发送消息
 * 队列：消息只能存在于队列中，只受内存和磁盘大小的限制 多个生产者可向一个队列发送消息，多个消费者可从一个队列接收消息
 * 消费着：等待接收消息的程序
 * 生产者、消费者、代理（broker）不需要在同一个主机上
 */
public class Send {

    //命名队列
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        //创建到服务器的连接
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             //创建 channel
             Channel channel = connection.createChannel()) {

           //声明队列
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            String message = "Hello World!";

            //队列只有在不存在的情况下才会被创建，消息的内容是字节数组，可以设置为任意编码格式
            // 第一个参数是消息交换机名称  空字符串表示默认或未命名的消息交换机：消息会被路由到指定的routingkey名称的队列，如果它存在的话。
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }






}
