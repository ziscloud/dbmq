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
package com.neuronbit.dbmq.client.producer;

import com.neuronbit.dbmq.client.ClientConfig;
import com.neuronbit.dbmq.client.MQClientAPI;
import com.neuronbit.dbmq.client.impl.CommunicationMode;
import com.neuronbit.dbmq.client.impl.MQClientManager;
import com.neuronbit.dbmq.client.impl.factory.MQClientInstance;
import com.neuronbit.dbmq.client.impl.producer.DefaultMQProducerImpl;
import com.neuronbit.dbmq.common.message.Message;
import com.neuronbit.dbmq.common.protocol.header.SendMessageRequestHeader;
import com.neuronbit.dbmq.common.protocol.route.QueueData;
import com.neuronbit.dbmq.common.protocol.route.TopicRouteData;
import com.neuronbit.dbmq.exception.MQClientException;
import com.neuronbit.dbmq.exception.RemotingTooMuchRequestException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQProducerTest {
    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPI mQClientAPIImpl;

    private DefaultMQProducer producer;
    private Message message;
    private Message zeroMsg;
    private Message bigMessage;
    private String topic = "FooBar";
    private String producerGroupPrefix = "FooBar_PID";

    public DefaultMQProducerTest() throws MQClientException {
    }

    @Before
    public void init() throws Exception {
        String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
        producer = new DefaultMQProducer(producerGroupTemp);
        message = new Message(topic, "a");
        zeroMsg = new Message(topic, "");
        bigMessage = new Message(topic, "This is a very huge message!");

        producer.start();

        Field field = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(producer.getDefaultMQProducerImpl(), mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        producer.getDefaultMQProducerImpl().getmQClientFactory().registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

        when(mQClientAPIImpl.sendMessage(any(Message.class),
                any(SendMessageRequestHeader.class),
                anyLong(),
                any(CommunicationMode.class),
                any(DefaultMQProducerImpl.class))
        ).thenReturn(createSendResult(SendStatus.SEND_OK));
    }

    @After
    public void terminate() {
        producer.shutdown();
    }

    @Test
    public void testSendMessage_ZeroMessage() throws InterruptedException, RemotingTooMuchRequestException {
        try {
            producer.send(zeroMsg);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("message body length is zero");
        }
    }

    @Test
    public void testSendMessage_NoRoute() throws InterruptedException, RemotingTooMuchRequestException {
        try {
            producer.send(message);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("No route info of this topic");
        }
    }

    @Test
    public void testSendMessageSync_Success() throws RemotingTooMuchRequestException, InterruptedException, MQClientException, SQLException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendResult sendResult = producer.send(message);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
    }

    @Test
    public void testSendMessageSync_WithBodyCompressed() throws RemotingTooMuchRequestException, InterruptedException, MQClientException, SQLException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendResult sendResult = producer.send(bigMessage);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
    }


    public static TopicRouteData createTopicRoute() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSynFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);
        return topicRouteData;
    }

    private SendResult createSendResult(SendStatus sendStatus) {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("123");
        sendResult.setOffsetMsgId("123");
        sendResult.setQueueOffset(456);
        sendResult.setSendStatus(sendStatus);
        sendResult.setRegionId("HZ");
        return sendResult;
    }

    private Throwable assertInOtherThread(final Runnable runnable) {
        final Throwable[] assertionErrors = new Throwable[1];
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runnable.run();
                } catch (AssertionError e) {
                    assertionErrors[0] = e;
                }
            }
        });
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            assertionErrors[0] = e;
        }
        return assertionErrors[0];
    }
}
