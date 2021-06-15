/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.neuronbit.dbmq.client.impl.consumer;

import com.neuronbit.dbmq.client.consumer.AllocateMessageQueueStrategy;
import com.neuronbit.dbmq.client.impl.factory.MQClientInstance;
import com.neuronbit.dbmq.common.MixAll;
import com.neuronbit.dbmq.common.constant.LoggerName;
import com.neuronbit.dbmq.common.message.MessageQueue;
import com.neuronbit.dbmq.common.protocol.body.LockBatchRequestBody;
import com.neuronbit.dbmq.common.protocol.body.UnlockBatchRequestBody;
import com.neuronbit.dbmq.common.protocol.heartbeat.ConsumeType;
import com.neuronbit.dbmq.common.protocol.heartbeat.MessageModel;
import com.neuronbit.dbmq.common.protocol.heartbeat.SubscriptionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class RebalanceImpl {
    protected final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);

    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner = new ConcurrentHashMap<>();
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<>(64);

    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
                         AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                         MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void doRebalance(final boolean isOrder) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    this.rebalanceByTopic(topic, isOrder);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }

    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
        List<String> cidAll = this.mQClientFactory.findConsumerIdList(consumerGroup);
        if (null == mqSet) {
            if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
            }
        }

        if (null == cidAll) {
            log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
        }

        if (mqSet != null && cidAll != null) {
            List<MessageQueue> mqAll = new ArrayList<>();
            mqAll.addAll(mqSet);

            Collections.sort(mqAll);
            Collections.sort(cidAll);

            AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

            List<MessageQueue> allocateResult = null;
            try {
                allocateResult = strategy.allocate(
                        this.consumerGroup,
                        this.mQClientFactory.getClientId(),
                        mqAll,
                        cidAll);
            } catch (Throwable e) {
                log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                        e);
                return;
            }

            Set<MessageQueue> allocateResultSet = new HashSet<>();
            if (allocateResult != null) {
                allocateResultSet.addAll(allocateResult);
            }

            boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
            if (changed) {
                log.info(
                        "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                        strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                        allocateResultSet.size(), allocateResultSet);
                this.messageQueueChanged(topic, mqSet, allocateResultSet);
            }
        }
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
                                                       final boolean isOrder) {
        boolean changed = false;

        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            if (mq.getTopic().equals(topic)) {
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        it.remove();
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                } else if (pq.isPullExpired()) {
                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY:
                            break;
                        case CONSUME_PASSIVELY:
                            pq.setDropped(true);
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove();
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                        consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        List<PullRequest> pullRequestList = new ArrayList<>();
        for (MessageQueue mq : mqSet) {
            if (!this.processQueueTable.containsKey(mq)) {
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                this.removeDirtyOffset(mq);
                ProcessQueue pq = new ProcessQueue();
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        this.dispatchPullRequest(pullRequestList);

        return changed;
    }

    public boolean lock(final MessageQueue mq) {
        LockBatchRequestBody requestBody = new LockBatchRequestBody();
        requestBody.setConsumerGroup(this.consumerGroup);
        requestBody.setClientId(this.mQClientFactory.getClientId());
        requestBody.getMqSet().add(mq);

        try {
            Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(requestBody, 1000);
            for (MessageQueue messageQueue : lockedMq) {
                ProcessQueue processQueue = this.processQueueTable.get(messageQueue);
                if (processQueue != null) {
                    processQueue.setLocked(true);
                    processQueue.setLastLockTimestamp(System.currentTimeMillis());
                }
            }

            boolean lockOK = lockedMq.contains(mq);
            log.info("the message queue lock {}, {} {}",
                    lockOK ? "OK" : "Failed",
                    this.consumerGroup,
                    mq);
            return lockOK;
        } catch (Exception e) {
            log.error("lockBatchMQ exception, " + mq, e);
        }

        return false;
    }

    public void destroy() {
        for (Map.Entry<MessageQueue, ProcessQueue> next : this.processQueueTable.entrySet()) {
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }

    private Set<MessageQueue> buildProcessQueueTableByBrokerName() {
        Set<MessageQueue> result = new HashSet<>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            result.add(mq);
        }

        return result;
    }

    public void lockAll() {
        final Set<MessageQueue> mqs = this.buildProcessQueueTableByBrokerName();

        if (mqs.isEmpty()) {
            return;
        }

        LockBatchRequestBody requestBody = new LockBatchRequestBody();
        requestBody.setConsumerGroup(this.consumerGroup);
        requestBody.setClientId(this.mQClientFactory.getClientId());
        requestBody.setMqSet(mqs);

        try {
            Set<MessageQueue> lockOKMQSet = this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(requestBody, 1000);

            for (MessageQueue mq : lockOKMQSet) {
                ProcessQueue processQueue = this.processQueueTable.get(mq);
                if (processQueue != null) {
                    if (!processQueue.isLocked()) {
                        log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                    }

                    processQueue.setLocked(true);
                    processQueue.setLastLockTimestamp(System.currentTimeMillis());
                }
            }
            for (MessageQueue mq : mqs) {
                if (!lockOKMQSet.contains(mq)) {
                    ProcessQueue processQueue = this.processQueueTable.get(mq);
                    if (processQueue != null) {
                        processQueue.setLocked(false);
                        log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                    }
                }
            }
        } catch (Exception e) {
            log.error("lockBatchMQ exception, " + mqs, e);
        }
    }

    public void unlockAll(final boolean oneway) {
        final Set<MessageQueue> mqs = buildProcessQueueTableByBrokerName();

        if (mqs.isEmpty()) {
            return;
        }

        UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
        requestBody.setConsumerGroup(this.consumerGroup);
        requestBody.setClientId(this.mQClientFactory.getClientId());
        requestBody.setMqSet(mqs);

        try {
            this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(requestBody, 1000, oneway);

            for (MessageQueue mq : mqs) {
                ProcessQueue processQueue = this.processQueueTable.get(mq);
                if (processQueue != null) {
                    processQueue.setLocked(false);
                    log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                }
            }
        } catch (Exception e) {
            log.error("unlockBatchMQ exception, " + mqs, e);
        }
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
        requestBody.setConsumerGroup(this.consumerGroup);
        requestBody.setClientId(this.mQClientFactory.getClientId());
        requestBody.getMqSet().add(mq);

        try {
            this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(requestBody, 1000, oneway);
            log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
        } catch (Exception e) {
            log.error("unlockBatchMQ exception, " + mq, e);
        }
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
                                             final Set<MessageQueue> mqDivided);

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }


    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }
}
