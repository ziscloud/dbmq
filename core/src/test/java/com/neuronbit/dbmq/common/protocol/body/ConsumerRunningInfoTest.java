/*
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
 */

package com.neuronbit.dbmq.common.protocol.body;

import com.neuronbit.dbmq.common.message.MessageQueue;
import com.neuronbit.dbmq.common.protocol.heartbeat.SubscriptionData;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.neuronbit.dbmq.common.protocol.heartbeat.ConsumeType.CONSUME_ACTIVELY;
import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerRunningInfoTest {

    private ConsumerRunningInfo consumerRunningInfo;

    private TreeMap<String, ConsumerRunningInfo> criTable;

    private MessageQueue messageQueue;

    @Before
    public void init() {
        consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.setJstack("test");

        TreeMap<MessageQueue, ProcessQueueInfo> mqTable = new TreeMap<MessageQueue, ProcessQueueInfo>();
        messageQueue = new MessageQueue("topicA", 1);
        mqTable.put(messageQueue, new ProcessQueueInfo());
        consumerRunningInfo.setMqTable(mqTable);

        TreeMap<String, ConsumeStatus> statusTable = new TreeMap<String, ConsumeStatus>();
        statusTable.put("topicA", new ConsumeStatus());
        consumerRunningInfo.setStatusTable(statusTable);

        TreeSet<SubscriptionData> subscriptionSet = new TreeSet<SubscriptionData>();
        subscriptionSet.add(new SubscriptionData());
        consumerRunningInfo.setSubscriptionSet(subscriptionSet);

        Properties properties = new Properties();
        properties.put(ConsumerRunningInfo.PROP_CONSUME_TYPE, CONSUME_ACTIVELY);
        properties.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, System.currentTimeMillis());
        consumerRunningInfo.setProperties(properties);

        criTable = new TreeMap<String, ConsumerRunningInfo>();
        criTable.put("client_id", consumerRunningInfo);
    }

    @Test
    public void testAnalyzeRebalance() {
        boolean result = ConsumerRunningInfo.analyzeRebalance(criTable);
        assertThat(result).isTrue();
    }

    @Test
    public void testAnalyzeProcessQueue() {
        String result = ConsumerRunningInfo.analyzeProcessQueue("client_id", consumerRunningInfo);
        assertThat(result).isEmpty();

    }

    @Test
    public void testAnalyzeSubscription() {
        boolean result = ConsumerRunningInfo.analyzeSubscription(criTable);
        assertThat(result).isTrue();
    }


}
