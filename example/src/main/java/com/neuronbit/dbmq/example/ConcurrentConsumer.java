/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.neuronbit.dbmq.example;

import com.alibaba.druid.pool.DruidDataSource;
import com.neuronbit.dbmq.client.consumer.DefaultMQPushConsumer;
import com.neuronbit.dbmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.neuronbit.dbmq.client.consumer.listener.MessageListenerConcurrently;
import com.neuronbit.dbmq.exception.MQClientException;

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
