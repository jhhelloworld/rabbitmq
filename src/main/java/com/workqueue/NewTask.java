package com.workqueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 在多个worker之间分配耗时的任务
 * 工作队列：针对一些资源紧张的并且必须等待完成的任务，把任务封装成一条消息，并且发送到队列中，Worker在后台完成这些任务，这些任务在worker之间共享
 */
public class NewTask {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            //队列持久性声明 RabbitMQ 服务停止，消息不丢失
            boolean durable = true;
            channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);

            String message = getMessage(argv);

            //消息持久性声明  PERSISTENT_TEXT_PLAIN
            channel.basicPublish("", TASK_QUEUE_NAME,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }

    /**
     * 发送一个字符串代表复杂消息，字符串中的“.”的数量代表复杂度
     * @param strings
     * @return
     */
    private static String getMessage(String[] strings) {
        if (strings.length < 1){
            return "Hello World!";
        }

        return String.join(" ", strings);
    }

}
