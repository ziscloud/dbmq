package com.neuronbit.lrdatf.example;

import com.alibaba.druid.pool.DruidDataSource;
import com.neuronbit.lrdatf.client.producer.DefaultMQProducer;
import com.neuronbit.lrdatf.client.producer.SendResult;
import com.neuronbit.lrdatf.common.message.Message;

public class SyncProducer {
    public static void main(String[] args) throws Exception {
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

        //Instantiate with a producer group name.
        DefaultMQProducer producer = new
                DefaultMQProducer("please_rename_unique_group_name");
        // Specify name server addresses.
        producer.setDataSource(dataSource);
        //Launch the instance.
        producer.start();
        for (int i = 0; i < 10; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    "Hello RocketMQ " + i /* Message body */
            );
            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}
