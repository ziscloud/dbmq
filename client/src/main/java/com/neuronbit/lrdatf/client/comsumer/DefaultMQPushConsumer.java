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
package com.neuronbit.lrdatf.client.comsumer;

import com.neuronbit.lrdatf.client.ClientConfig;
import com.neuronbit.lrdatf.client.comsumer.listener.MessageListener;
import com.neuronbit.lrdatf.client.comsumer.listener.MessageListenerConcurrently;
import com.neuronbit.lrdatf.client.comsumer.listener.MessageListenerOrderly;
import com.neuronbit.lrdatf.client.comsumer.rebalance.AllocateMessageQueueAveragely;
import com.neuronbit.lrdatf.client.comsumer.store.OffsetStore;
import com.neuronbit.lrdatf.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.neuronbit.lrdatf.common.MixAll;
import com.neuronbit.lrdatf.common.UtilAll;
import com.neuronbit.lrdatf.common.constant.LoggerName;
import com.neuronbit.lrdatf.common.consumer.ConsumeFromWhere;
import com.neuronbit.lrdatf.common.message.MessageExt;
import com.neuronbit.lrdatf.common.message.MessageQueue;
import com.neuronbit.lrdatf.common.protocol.NamespaceUtil;
import com.neuronbit.lrdatf.common.protocol.heartbeat.MessageModel;
import com.neuronbit.lrdatf.exception.MQClientException;
import com.neuronbit.lrdatf.exception.RemotingTooMuchRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * In most scenarios, this is the mostly recommended class to consume messages.
 * </p>
 * <p>
 * Technically speaking, this push client is virtually a wrapper of the underlying pull service. Specifically, on
 * arrival of messages pulled from brokers, it roughly invokes the registered callback handler to feed the messages.
 * </p>
 * <p>
 * See quickstart/Consumer in the example module for a typical usage.
 * </p>
 *
 * <p>
 * <strong>Thread Safety:</strong> After initialization, the instance can be regarded as thread-safe.
 * </p>
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {
    private final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);

    /**
     * Internal implementation. Most of the functions herein are delegated to it.
     */
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    /**
     * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
     * load balance. It's required and needs to be globally unique.
     * </p>
     * <p>
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
     */
    private String consumerGroup;

    /**
     * Message model defines the way how messages are delivered to each consumer clients.
     * </p>
     * <p>
     * RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
     * the same {@link #consumerGroup} would only consume shards of the messages subscribed, which achieves load
     * balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
     * separately.
     * </p>
     * <p>
     * This field defaults to clustering.
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * Consuming point on consumer booting.
     * </p>
     * <p>
     * There are three consuming points:
     * <ul>
     * <li>
     * <code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it stopped previously.
     * If it were a newly booting up consumer client, according aging of the consumer group, there are two
     * cases:
     * <ol>
     * <li>
     * if the consumer group is created so recently that the earliest message being subscribed has yet
     * expired, which means the consumer group represents a lately launched business, consuming will
     * start from the very beginning;
     * </li>
     * <li>
     * if the earliest message being subscribed has expired, consuming will start from the latest
     * messages, meaning messages born prior to the booting timestamp would be ignored.
     * </li>
     * </ol>
     * </li>
     * <li>
     * <code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from earliest messages available.
     * </li>
     * <li>
     * <code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified timestamp, which means
     * messages born prior to {@link #consumeTimestamp} will be ignored
     * </li>
     * </ul>
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * Backtracking consumption time with second precision. Time format is
     * 20131223171201<br>
     * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
     * Default backtracking consumption time Half an hour ago.
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

    /**
     * Queue allocation algorithm specifying how message queues are allocated to each consumer clients.
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * Subscription relationship
     */
    private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();

    /**
     * Message listener
     */
    private MessageListener messageListener;

    /**
     * Offset Storage
     */
    private OffsetStore offsetStore;

    /**
     * Minimum consumer thread number
     */
    private int consumeThreadMin = 20;

    /**
     * Max consumer thread number
     */
    private int consumeThreadMax = 20;

    /**
     * Threshold for dynamic adjustment of the number of thread pool
     */
    private long adjustThreadPoolNumsThreshold = 100000;

    /**
     * Concurrently max span offset.it has no effect on sequential consumption
     */
    private int consumeConcurrentlyMaxSpan = 2000;

    /**
     * Flow control threshold on queue level, each message queue will cache at most 1000 messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     */
    private int pullThresholdForQueue = 1000;

    /**
     * Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     *
     * <p>
     * The size of a message only measured by message body, so it's not accurate
     */
    private int pullThresholdSizeForQueue = 100;

    /**
     * Flow control threshold on topic level, default value is -1(Unlimited)
     * <p>
     * The value of {@code pullThresholdForQueue} will be overwrote and calculated based on
     * {@code pullThresholdForTopic} if it is't unlimited
     * <p>
     * For example, if the value of pullThresholdForTopic is 1000 and 10 message queues are assigned to this consumer,
     * then pullThresholdForQueue will be set to 100
     */
    private int pullThresholdForTopic = -1;

    /**
     * Limit the cached message size on topic level, default value is -1 MiB(Unlimited)
     * <p>
     * The value of {@code pullThresholdSizeForQueue} will be overwrote and calculated based on
     * {@code pullThresholdSizeForTopic} if it is't unlimited
     * <p>
     * For example, if the value of pullThresholdSizeForTopic is 1000 MiB and 10 message queues are
     * assigned to this consumer, then pullThresholdSizeForQueue will be set to 100 MiB
     */
    private int pullThresholdSizeForTopic = -1;

    /**
     * Message pull Interval
     */
    private long pullInterval = 0;

    /**
     * Batch consumption size
     */
    private int consumeMessageBatchMaxSize = 1;

    /**
     * Batch pull size
     */
    private int pullBatchSize = 32;

    /**
     * Whether update subscription relationship when every pull
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    /**
     * Max re-consume times. -1 means 16 times.
     * </p>
     * <p>
     * If messages are re-consumed more than {@link #maxReconsumeTimes} before success, it's be directed to a deletion
     * queue waiting.
     */
    private int maxReconsumeTimes = -1;

    /**
     * Suspending pulling time for cases requiring slow pulling like flow-control scenario.
     */
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     */
    private long consumeTimeout = 15;

    /**
     * Maximum time to await message consuming when shutdown consumer, 0 indicates no await.
     */
    private long awaitTerminationMillisWhenShutdown = 0;

    /**
     * Interface of asynchronous transfer data
     */
//    private TraceDispatcher traceDispatcher = null;

    /**
     * Default constructor.
     */
    public DefaultMQPushConsumer() {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying consumer group.
     *
     * @param consumerGroup Consumer group.
     */
    public DefaultMQPushConsumer(final String consumerGroup) {
        this(null, consumerGroup, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying namespace and consumer group.
     *
     * @param namespace     Namespace for this MQ Producer instance.
     * @param consumerGroup Consumer group.
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup) {
        this(namespace, consumerGroup, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying consumer group, RPC hook and message queue allocating algorithm.
     *
     * @param consumerGroup                Consume queue.
     * @param rpcHook                      RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy Message queue allocating algorithm.
     */
    public DefaultMQPushConsumer(final String consumerGroup, AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this(null, consumerGroup, allocateMessageQueueStrategy);
    }

    /**
     * Constructor specifying namespace, consumer group, RPC hook and message queue allocating algorithm.
     *
     * @param namespace                    Namespace for this MQ Producer instance.
     * @param consumerGroup                Consume queue.
     * @param rpcHook                      RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy Message queue allocating algorithm.
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup,
                                 AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.consumerGroup = consumerGroup;
        this.namespace = namespace;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this);
    }

    /**
     * Constructor specifying consumer group and enabled msg trace flag.
     *
     * @param consumerGroup  Consumer group.
     * @param enableMsgTrace Switch flag instance for message trace.
     */
    public DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace) {
        this(null, consumerGroup, new AllocateMessageQueueAveragely(), enableMsgTrace, null);
    }

    /**
     * Constructor specifying consumer group, enabled msg trace flag and customized trace topic name.
     *
     * @param consumerGroup        Consumer group.
     * @param enableMsgTrace       Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(null, consumerGroup, new AllocateMessageQueueAveragely(), enableMsgTrace, customizedTraceTopic);
    }


    /**
     * Constructor specifying consumer group, RPC hook, message queue allocating algorithm, enabled msg trace flag and customized trace topic name.
     *
     * @param consumerGroup                Consume queue.
     * @param rpcHook                      RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy message queue allocating algorithm.
     * @param enableMsgTrace               Switch flag instance for message trace.
     * @param customizedTraceTopic         The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQPushConsumer(final String consumerGroup,
                                 AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(null, consumerGroup, allocateMessageQueueStrategy, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying namespace, consumer group, RPC hook, message queue allocating algorithm, enabled msg trace flag and customized trace topic name.
     *
     * @param namespace                    Namespace for this MQ Producer instance.
     * @param consumerGroup                Consume queue.
     * @param rpcHook                      RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy message queue allocating algorithm.
     * @param enableMsgTrace               Switch flag instance for message trace.
     * @param customizedTraceTopic         The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup,
                                 AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic) {
        this.consumerGroup = consumerGroup;
        this.namespace = namespace;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this);
//        if (enableMsgTrace) {
//            try {
//                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(consumerGroup, TraceDispatcher.Type.CONSUME, customizedTraceTopic);
//                dispatcher.setHostConsumer(this.getDefaultMQPushConsumerImpl());
//                traceDispatcher = dispatcher;
//                this.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(
//                        new ConsumeMessageTraceHookImpl(traceDispatcher));
//            } catch (Throwable e) {
//                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
//            }
//        }
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }

    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public int getPullThresholdForTopic() {
        return pullThresholdForTopic;
    }

    public void setPullThresholdForTopic(final int pullThresholdForTopic) {
        this.pullThresholdForTopic = pullThresholdForTopic;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(final int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
    }

    public int getPullThresholdSizeForTopic() {
        return pullThresholdSizeForTopic;
    }

    public void setPullThresholdSizeForTopic(final int pullThresholdSizeForTopic) {
        this.pullThresholdSizeForTopic = pullThresholdSizeForTopic;
    }

    public Map<String, String> getSubscription() {
        return subscription;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public void setSubscription(Map<String, String> subscription) {
        Map<String, String> subscriptionWithNamespace = new HashMap<String, String>();
        for (String topic : subscription.keySet()) {
            subscriptionWithNamespace.put(withNamespace(topic), subscription.get(topic));
        }
        this.subscription = subscriptionWithNamespace;
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPushConsumerImpl.fetchSubscribeMessageQueues(withNamespace(topic));
    }

    /**
     * This method gets internal infrastructure readily to serve. Instances must call this method after configuration.
     *
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void start() throws MQClientException {
        setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        this.defaultMQPushConsumerImpl.start();
//        if (null != traceDispatcher) {
//            try {
//                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
//            } catch (MQClientException e) {
//                log.warn("trace dispatcher start failed ", e);
//            }
//        }
    }

    /**
     * Shut down this client and releasing underlying resources.
     */
    @Override
    public void shutdown() {
        this.defaultMQPushConsumerImpl.shutdown(awaitTerminationMillisWhenShutdown);
//        if (null != traceDispatcher) {
//            traceDispatcher.shutdown();
//        }
    }

    /**
     * Register a callback to execute on message arrival for concurrent consuming.
     *
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Register a callback to execute on message arrival for orderly consuming.
     *
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Subscribe a topic to consuming subscription.
     *
     * @param topic         topic to subscribe.
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br>
     *                      if null or * expression,meaning subscribe all
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), subExpression);
    }

//
//    /**
//     * Subscribe a topic by message selector.
//     *
//     * @param topic           topic to consume.
//     * @param messageSelector {@link org.apache.rocketmq.client.consumer.MessageSelector}
//     * @see org.apache.rocketmq.client.consumer.MessageSelector#bySql
//     * @see org.apache.rocketmq.client.consumer.MessageSelector#byTag
//     */
//    @Override
//    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
//        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), messageSelector);
//    }

    /**
     * Un-subscribe the specified topic from subscription.
     *
     * @param topic message topic
     */
    @Override
    public void unsubscribe(String topic) {
        this.defaultMQPushConsumerImpl.unsubscribe(topic);
    }

    /**
     * Update the message consuming thread core pool size.
     *
     * @param corePoolSize new core pool size.
     */
    @Override
    public void updateCorePoolSize(int corePoolSize) {
        this.defaultMQPushConsumerImpl.updateCorePoolSize(corePoolSize);
    }

    /**
     * Suspend pulling new messages.
     */
    @Override
    public void suspend() {
        this.defaultMQPushConsumerImpl.suspend();
    }

    /**
     * Resume pulling.
     */
    @Override
    public void resume() {
        this.defaultMQPushConsumerImpl.resume();
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }

    public boolean isPostSubscriptionWhenPull() {
        return postSubscriptionWhenPull;
    }

    public void setPostSubscriptionWhenPull(boolean postSubscriptionWhenPull) {
        this.postSubscriptionWhenPull = postSubscriptionWhenPull;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public long getAdjustThreadPoolNumsThreshold() {
        return adjustThreadPoolNumsThreshold;
    }

    public void setAdjustThreadPoolNumsThreshold(long adjustThreadPoolNumsThreshold) {
        this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(final long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(final long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public long getAwaitTerminationMillisWhenShutdown() {
        return awaitTerminationMillisWhenShutdown;
    }

    public void setAwaitTerminationMillisWhenShutdown(long awaitTerminationMillisWhenShutdown) {
        this.awaitTerminationMillisWhenShutdown = awaitTerminationMillisWhenShutdown;
    }

    public boolean isConsumeOrderly() {
        return this.defaultMQPushConsumerImpl.isConsumeOrderly();
    }

//    public TraceDispatcher getTraceDispatcher() {
//        return traceDispatcher;
//    }


    /**
     * Send message back to broker which will be re-delivered in future.
     *
     * @param msg        Message to send back.
     * @param delayLevel delay level.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQClientException    if there is any client error.
     */
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel) throws InterruptedException, MQClientException, RemotingTooMuchRequestException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel);
    }
}
