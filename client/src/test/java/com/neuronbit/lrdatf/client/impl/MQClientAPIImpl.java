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

package com.neuronbit.lrdatf.client.impl;

import com.neuronbit.lrdatf.client.ClientConfig;
import com.neuronbit.lrdatf.client.MQClientAPI;
import com.neuronbit.lrdatf.client.consumer.PullCallback;
import com.neuronbit.lrdatf.client.consumer.PullResult;
import com.neuronbit.lrdatf.client.impl.factory.MQClientInstance;
import com.neuronbit.lrdatf.client.impl.producer.DefaultMQProducerImpl;
import com.neuronbit.lrdatf.client.impl.producer.TopicPublishInfo;
import com.neuronbit.lrdatf.client.producer.SendCallback;
import com.neuronbit.lrdatf.client.producer.SendResult;
import com.neuronbit.lrdatf.common.message.Message;
import com.neuronbit.lrdatf.common.message.MessageExt;
import com.neuronbit.lrdatf.common.message.MessageQueue;
import com.neuronbit.lrdatf.common.protocol.body.LockBatchRequestBody;
import com.neuronbit.lrdatf.common.protocol.body.UnlockBatchRequestBody;
import com.neuronbit.lrdatf.common.protocol.header.PullMessageRequestHeader;
import com.neuronbit.lrdatf.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.neuronbit.lrdatf.common.protocol.header.SendMessageRequestHeader;
import com.neuronbit.lrdatf.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.neuronbit.lrdatf.common.protocol.heartbeat.HeartbeatData;
import com.neuronbit.lrdatf.common.protocol.heartbeat.SubscriptionData;
import com.neuronbit.lrdatf.common.protocol.route.TopicRouteData;
import com.neuronbit.lrdatf.exception.MQBrokerException;
import com.neuronbit.lrdatf.exception.MQClientException;
import com.neuronbit.lrdatf.exception.RemotingTooMuchRequestException;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class MQClientAPIImpl implements MQClientAPI {
    @Override
    public void start() {

    }

    @Override
    public int sendHeartbeat(HeartbeatData heartbeatData, long timeoutMillis) throws SQLException {
        return 0;
    }

    @Override
    public void unregisterClient(String clientID, String producerGroup, String consumerGroup, long timeoutMillis) throws InterruptedException, SQLException {

    }

    @Override
    public SendResult sendMessage(Message msg, SendMessageRequestHeader requestHeader, long timeoutMillis, CommunicationMode communicationMode, DefaultMQProducerImpl producer) throws InterruptedException, RemotingTooMuchRequestException, MQClientException {
        return null;
    }

    @Override
    public SendResult sendMessage(Message msg, SendMessageRequestHeader requestHeader, long timeoutMillis, CommunicationMode communicationMode, SendCallback sendCallback, TopicPublishInfo topicPublishInfo, MQClientInstance instance, int retryTimesWhenSendFailed, DefaultMQProducerImpl producer) throws RemotingTooMuchRequestException, MQClientException {
        return null;
    }

    @Override
    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(String topic, long timeoutMillis) throws MQClientException, InterruptedException, SQLException {
        return null;
    }

    @Override
    public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis) throws MQClientException, InterruptedException, SQLException {
        return null;
    }

    @Override
    public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis, boolean allowTopicNotExist) throws SQLException {
        return null;
    }

    @Override
    public void checkClientInBroker(String consumerGroup, String clientId, SubscriptionData subscriptionData, long timeoutMillis) throws MQClientException, SQLException {

    }

    @Override
    public List<String> getConsumerIdListByGroup(String consumerGroup, long timeoutMillis) throws SQLException {
        return null;
    }

    @Override
    public void consumerSendMessageBack(MessageExt msg, String consumerGroup, int delayLevel, long timeoutMillis, int maxConsumeRetryTimes) throws SQLException {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public PullResult pullMessage(PullMessageRequestHeader requestHeader, long timeoutMillis, CommunicationMode communicationMode, PullCallback pullCallback) throws MQBrokerException, SQLException {
        return null;
    }

    @Override
    public long queryConsumerOffset(QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis) throws MQBrokerException, SQLException {
        return 0;
    }

    @Override
    public void updateConsumerOffset(UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis) throws MQBrokerException, SQLException {

    }

    @Override
    public long searchOffset(String topic, int queueId, long timestamp, long timeoutMillis) throws MQBrokerException, SQLException {
        return 0;
    }

    @Override
    public long getMaxOffset(String topic, int queueId, long timeoutMillis) throws SQLException {
        return 0;
    }

    @Override
    public long getMinOffset(String topic, int queueId, long timeoutMillis) throws SQLException {
        return 0;
    }

    @Override
    public long getEarliestMsgStoretime(String topic, int queueId, long timeoutMillis) throws SQLException, MQBrokerException {
        return 0;
    }

    @Override
    public Set<MessageQueue> lockBatchMQ(LockBatchRequestBody requestBody, long timeoutMillis) {
        return null;
    }

    @Override
    public Set<MessageQueue> unlockBatchMQ(UnlockBatchRequestBody requestBody, long timeoutMillis, boolean oneway) {
        return null;
    }

    @Override
    public boolean tryLock(String lockId, String lockValue, long timeoutMillis) {
        return false;
    }

    @Override
    public boolean unlock(String lockId, String lockValue, long timeoutMillis) {
        return false;
    }

    @Override
    public void scanNotActiveProducer() {

    }

    @Override
    public void scanNotActiveConsumer() {

    }

    @Override
    public void setClientConfig(ClientConfig clientConfig) {

    }
}
