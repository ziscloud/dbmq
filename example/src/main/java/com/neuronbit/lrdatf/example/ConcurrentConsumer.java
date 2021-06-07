package com.neuronbit.lrdatf.example;

import com.alibaba.druid.pool.DruidDataSource;
import com.neuronbit.lrdatf.client.consumer.DefaultMQPushConsumer;
import com.neuronbit.lrdatf.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.neuronbit.lrdatf.client.consumer.listener.MessageListenerConcurrently;
import com.neuronbit.lrdatf.exception.MQClientException;

public class ConcurrentConsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DruidDataSource dataSource = new DruidDataSource();
        // 基本属性 url、user、password
        dataSource.setUrl("jdbc:mysql://localhost:3306/message_queue");
        dataSource.setUsername("root");
        dataSource.setPassword("12345678");

        // 配置初始化大小、最小、最大
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(10);

        // 配置获取连接等待超时的时间
        dataSource.setMaxWait(6000);

        // 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
        dataSource.setTimeBetweenEvictionRunsMillis(2000);

        // 配置一个连接在池中最小生存的时间，单位是毫秒
        dataSource.setMinEvictableIdleTimeMillis(600000);
        dataSource.setMaxEvictableIdleTimeMillis(900000);

        dataSource.setValidationQuery("select 1");
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);

        dataSource.setKeepAlive(true);
        dataSource.setPhyMaxUseCount(500);

        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

        // Specify name server addresses.
        consumer.setDataSource(dataSource);

        consumer.setPullInterval(1000 * 1);
        //consumer.setPersistConsumerOffsetInterval();
        consumer.setMaxReconsumeTimes(3);

        // Subscribe one more more topics to consume.
        consumer.subscribe("TopicTest", "*");
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
