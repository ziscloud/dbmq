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
package com.neuronbit.dbmq.client.consumer.store;

import com.neuronbit.dbmq.client.ClientConfig;
import com.neuronbit.dbmq.client.MQClientAPI;
import com.neuronbit.dbmq.client.impl.factory.MQClientInstance;
import com.neuronbit.dbmq.common.message.MessageQueue;
import com.neuronbit.dbmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.neuronbit.dbmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.neuronbit.dbmq.exception.MQBrokerException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RemoteBrokerOffsetStoreTest {
    @Mock
    private MQClientInstance mQClientFactory;
    @Mock
    private MQClientAPI mqClientAPI;
    private String group = "FooBarGroup";
    private String topic = "FooBar";

    @Before
    public void init() {
        System.setProperty("rocketmq.client.localOffsetStoreDir", System.getProperty("java.io.tmpdir") + ".rocketmq_offsets");
        String clientId = new ClientConfig().buildMQClientId() + "#TestNamespace" + System.currentTimeMillis();
        when(mQClientFactory.getClientId()).thenReturn(clientId);
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mqClientAPI);
    }

    @Test
    public void testUpdateOffset() throws Exception {
        OffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, 1);

        offsetStore.updateOffset(messageQueue, 1024, false);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1024);

        offsetStore.updateOffset(messageQueue, 1023, false);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1023);

        offsetStore.updateOffset(messageQueue, 1022, true);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1023);
    }

    @Test
    public void testReadOffset_WithException() throws Exception {
        OffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, 2);

        offsetStore.updateOffset(messageQueue, 1024, false);

        doThrow(new MQBrokerException(-1, "", null))
                .when(mqClientAPI).queryConsumerOffset(any(QueryConsumerOffsetRequestHeader.class), anyLong());
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(-1);
    }

    @Test
    public void testReadOffset_Success() throws Exception {
        OffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, group);
        final MessageQueue messageQueue = new MessageQueue(topic, 3);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                UpdateConsumerOffsetRequestHeader updateRequestHeader = mock.getArgument(0);
                when(mqClientAPI.queryConsumerOffset(any(QueryConsumerOffsetRequestHeader.class), anyLong()))
                        .thenReturn(updateRequestHeader.getCommitOffset());
                return null;
            }
        }).when(mqClientAPI).updateConsumerOffset(any(UpdateConsumerOffsetRequestHeader.class), any(Long.class));

        offsetStore.updateOffset(messageQueue, 1024, false);
        offsetStore.persist(messageQueue);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1024);

        offsetStore.updateOffset(messageQueue, 1023, false);
        offsetStore.persist(messageQueue);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1023);

        offsetStore.updateOffset(messageQueue, 1022, true);
        offsetStore.persist(messageQueue);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1023);

        offsetStore.updateOffset(messageQueue, 1025, false);
        offsetStore.persistAll(new HashSet<MessageQueue>(Collections.singletonList(messageQueue)));
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1025);
    }

    @Test
    public void testRemoveOffset() throws Exception {
        OffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, group);
        final MessageQueue messageQueue = new MessageQueue(topic, 4);

        offsetStore.updateOffset(messageQueue, 1024, false);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1024);

        offsetStore.removeOffset(messageQueue);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(-1);
    }
}