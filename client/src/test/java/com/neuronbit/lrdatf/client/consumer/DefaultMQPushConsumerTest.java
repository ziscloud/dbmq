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
package com.neuronbit.lrdatf.client.consumer;

import com.neuronbit.lrdatf.client.MQClientAPI;
import com.neuronbit.lrdatf.client.consumer.listener.*;
import com.neuronbit.lrdatf.client.impl.CommunicationMode;
import com.neuronbit.lrdatf.client.impl.consumer.*;
import com.neuronbit.lrdatf.client.impl.factory.MQClientInstance;
import com.neuronbit.lrdatf.common.message.MessageExt;
import com.neuronbit.lrdatf.common.message.MessageQueue;
import com.neuronbit.lrdatf.common.protocol.header.PullMessageRequestHeader;
import com.neuronbit.lrdatf.exception.MQClientException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DefaultMQPushConsumerImpl.class})
public class DefaultMQPushConsumerTest {
    private String consumerGroup;
    private String topic = "FooBar";
    private String brokerName = "BrokerA";
    private MQClientInstance mQClientFactory;

    @Mock
    private MQClientAPI mQClientAPIImpl;
    private PullAPIWrapper pullAPIWrapper;
    private RebalancePushImpl rebalancePushImpl;
    private DefaultMQPushConsumer pushConsumer;

    @Before
    public void init() throws Exception {
        consumerGroup = "FooBarGroup" + System.currentTimeMillis();
        pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.setPullInterval(60 * 1000);

        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                return null;
            }
        });

        DefaultMQPushConsumerImpl pushConsumerImpl = pushConsumer.getDefaultMQPushConsumerImpl();
        PowerMockito.suppress(PowerMockito.method(DefaultMQPushConsumerImpl.class, "updateTopicSubscribeInfoWhenSubscriptionChanged"));
        rebalancePushImpl = spy(new RebalancePushImpl(pushConsumer.getDefaultMQPushConsumerImpl()));
        Field field = DefaultMQPushConsumerImpl.class.getDeclaredField("rebalanceImpl");
        field.setAccessible(true);
        field.set(pushConsumerImpl, rebalancePushImpl);

        pushConsumer.subscribe(topic, "*");
        pushConsumer.start();

        mQClientFactory = spy(pushConsumerImpl.getmQClientFactory());
        field = DefaultMQPushConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(pushConsumerImpl, mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        pullAPIWrapper = spy(new PullAPIWrapper(mQClientFactory, consumerGroup, false));
        field = DefaultMQPushConsumerImpl.class.getDeclaredField("pullAPIWrapper");
        field.setAccessible(true);
        field.set(pushConsumerImpl, pullAPIWrapper);

        pushConsumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().setmQClientFactory(mQClientFactory);
        mQClientFactory.registerConsumer(consumerGroup, pushConsumerImpl);

        when(mQClientFactory.getMQClientAPIImpl().pullMessage(any(PullMessageRequestHeader.class),
                anyLong(), any(CommunicationMode.class), nullable(PullCallback.class)))
                .thenAnswer(new Answer<Object>() {
                    @Override
                    public Object answer(InvocationOnMock mock) throws Throwable {
                        PullMessageRequestHeader requestHeader = mock.getArgument(0);
                        MessageExt messageClientExt = new MessageExt();
                        messageClientExt.setTopic(topic);
                        messageClientExt.setQueueId(0);
                        messageClientExt.setMsgId("123");
                        messageClientExt.setBody("a");
                        messageClientExt.setMsgId("234");
                        messageClientExt.setBornHost(InetAddress.getByName("localhost"));
                        messageClientExt.setStoreHost(InetAddress.getByName("localhost"));
                        PullResult pullResult = createPullResult(requestHeader, PullStatus.FOUND, Collections.<MessageExt>singletonList(messageClientExt));
                        ((PullCallback) mock.getArgument(3)).onSuccess(pullResult);
                        return pullResult;
                    }
                });

        doReturn(Collections.singletonList(mQClientFactory.getClientId())).when(mQClientFactory).findConsumerIdList(anyString());
        Set<MessageQueue> messageQueueSet = new HashSet<MessageQueue>();
        messageQueueSet.add(createPullRequest().getMessageQueue());
        pushConsumer.getDefaultMQPushConsumerImpl().updateTopicSubscribeInfo(topic, messageQueueSet);
        doReturn(123L).when(rebalancePushImpl).computePullFromWhere(any(MessageQueue.class));
    }

    @After
    public void terminate() {
        pushConsumer.shutdown();
    }

    @Test
    public void testStart_OffsetShouldNotNUllAfterStart() {
        Assert.assertNotNull(pushConsumer.getOffsetStore());
    }

    @Test
    public void testPullMessage_Success() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MessageExt[] messageExts = new MessageExt[1];
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(
                new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(),
                        new MessageListenerConcurrently() {
                            @Override
                            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                                messageExts[0] = msgs.get(0);
                                countDownLatch.countDown();
                                return null;
                            }
                        }
                )
        );

        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        countDownLatch.await();
        assertThat(messageExts[0].getTopic()).isEqualTo(topic);
        assertThat(messageExts[0].getBody()).isEqualTo("a");
    }

    @Test
    public void testPullMessage_SuccessWithOrderlyService() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MessageExt[] messageExts = new MessageExt[1];

        MessageListenerOrderly listenerOrderly = new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                messageExts[0] = msgs.get(0);
                countDownLatch.countDown();
                return null;
            }
        };
        pushConsumer.registerMessageListener(listenerOrderly);
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), listenerOrderly));
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeOrderly(true);
        pushConsumer.getDefaultMQPushConsumerImpl().doRebalance();
        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestLater(createPullRequest(), 100);

        countDownLatch.await(10, TimeUnit.SECONDS);
        assertThat(messageExts[0].getTopic()).isEqualTo(topic);
        assertThat(messageExts[0].getBody()).isEqualTo("a");
    }

    @Test
    public void testCheckConfig() {
        DefaultMQPushConsumer pushConsumer = createPushConsumer();

        pushConsumer.setPullThresholdForQueue(65535 + 1);
        try {
            pushConsumer.start();
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("pullThresholdForQueue Out of range [1, 65535]");
        }

        pushConsumer = createPushConsumer();
        pushConsumer.setPullThresholdForTopic(65535 * 100 + 1);

        try {
            pushConsumer.start();
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("pullThresholdForTopic Out of range [1, 6553500]");
        }

        pushConsumer = createPushConsumer();
        pushConsumer.setPullThresholdSizeForQueue(1024 + 1);
        try {
            pushConsumer.start();
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("pullThresholdSizeForQueue Out of range [1, 1024]");
        }

        pushConsumer = createPushConsumer();
        pushConsumer.setPullThresholdSizeForTopic(1024 * 100 + 1);
        try {
            pushConsumer.start();
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("pullThresholdSizeForTopic Out of range [1, 102400]");
        }
    }

    @Test
    public void testGracefulShutdown() throws InterruptedException, MQClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        pushConsumer.setAwaitTerminationMillisWhenShutdown(2000);
        final AtomicBoolean messageConsumedFlag = new AtomicBoolean(false);
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                countDownLatch.countDown();
                try {
                    Thread.sleep(1000);
                    messageConsumedFlag.set(true);
                } catch (InterruptedException e) {
                }

                return null;
            }
        }));

        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        countDownLatch.await();

        pushConsumer.shutdown();
        assertThat(messageConsumedFlag.get()).isTrue();
    }

    private DefaultMQPushConsumer createPushConsumer() {
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                return null;
            }
        });
        return pushConsumer;
    }

    private PullRequest createPullRequest() {
        PullRequest pullRequest = new PullRequest();
        pullRequest.setConsumerGroup(consumerGroup);
        pullRequest.setNextOffset(1024);

        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setQueueId(0);
        messageQueue.setTopic(topic);
        pullRequest.setMessageQueue(messageQueue);
        ProcessQueue processQueue = new ProcessQueue();
        processQueue.setLocked(true);
        processQueue.setLastLockTimestamp(System.currentTimeMillis());
        pullRequest.setProcessQueue(processQueue);

        return pullRequest;
    }

    private PullResult createPullResult(PullMessageRequestHeader requestHeader, PullStatus pullStatus,
                                        List<MessageExt> messageExtList) throws Exception {
        return new PullResult(pullStatus, requestHeader.getQueueOffset() + messageExtList.size(), 123, 2048, messageExtList);
    }
}