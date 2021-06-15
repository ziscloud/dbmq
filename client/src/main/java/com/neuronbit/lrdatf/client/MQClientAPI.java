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

package com.neuronbit.lrdatf.client;

import com.neuronbit.lrdatf.client.consumer.PullCallback;
import com.neuronbit.lrdatf.client.consumer.PullResult;
import com.neuronbit.lrdatf.client.impl.CommunicationMode;
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

public interface MQClientAPI {

    void start();

    int sendHeartbeat(HeartbeatData heartbeatData, long timeoutMillis) throws SQLException;

    void unregisterClient(String clientID,
                          String producerGroup,
                          String consumerGroup,
                          long timeoutMillis) throws InterruptedException, SQLException;

    SendResult sendMessage(Message msg,
                           SendMessageRequestHeader requestHeader,
                           long timeoutMillis,
                           CommunicationMode communicationMode,
                           //            final SendMessageContext context,
                           DefaultMQProducerImpl producer) throws InterruptedException, RemotingTooMuchRequestException, MQClientException;

    SendResult sendMessage(Message msg,
                           SendMessageRequestHeader requestHeader,
                           long timeoutMillis,
                           CommunicationMode communicationMode,
                           SendCallback sendCallback,
                           TopicPublishInfo topicPublishInfo,
                           MQClientInstance instance,
                           int retryTimesWhenSendFailed,
                           //final SendMessageContext context,
                           DefaultMQProducerImpl producer) throws RemotingTooMuchRequestException, MQClientException;

    TopicRouteData getDefaultTopicRouteInfoFromNameServer(String topic, long timeoutMillis)
            throws MQClientException, InterruptedException, SQLException;

    TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis)
            throws MQClientException, InterruptedException, SQLException;

    TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis,
                                                   boolean allowTopicNotExist) throws SQLException;

    void checkClientInBroker(String consumerGroup,
                             String clientId, SubscriptionData subscriptionData,
                             long timeoutMillis) throws MQClientException, SQLException;

    List<String> getConsumerIdListByGroup(String consumerGroup,
                                          long timeoutMillis) throws SQLException;

    //    public ConsumerRunningInfo getConsumerRunningInfo(final String addr, String consumerGroup, String clientId,
//                                                      boolean jstack,
//                                                      final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
//        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
//        requestHeader.setConsumerGroup(consumerGroup);
//        requestHeader.setClientId(clientId);
//        requestHeader.setJstackEnable(jstack);
//
//        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
//
//        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
//                request, timeoutMillis);
//        assert response != null;
//        switch (response.getCode()) {
//            case ResponseCode.SUCCESS: {
//                byte[] body = response.getBody();
//                if (body != null) {
//                    ConsumerRunningInfo info = ConsumerRunningInfo.decode(body, ConsumerRunningInfo.class);
//                    return info;
//                }
//            }
//            default:
//                break;
//        }
//
//        throw new MQClientException(response.getCode(), response.getRemark());
//    }
//
//    public QueryConsumeQueueResponseBody queryConsumeQueue(final String brokerAddr, final String topic,
//                                                           final int queueId,
//                                                           final long index, final int count, final String consumerGroup,
//                                                           final long timeoutMillis) throws InterruptedException,
//            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {
//
//        QueryConsumeQueueRequestHeader requestHeader = new QueryConsumeQueueRequestHeader();
//        requestHeader.setTopic(topic);
//        requestHeader.setQueueId(queueId);
//        requestHeader.setIndex(index);
//        requestHeader.setCount(count);
//        requestHeader.setConsumerGroup(consumerGroup);
//
//        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_QUEUE, requestHeader);
//
//        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
//
//        assert response != null;
//
//        if (ResponseCode.SUCCESS == response.getCode()) {
//            return QueryConsumeQueueResponseBody.decode(response.getBody(), QueryConsumeQueueResponseBody.class);
//        }
//
//        throw new MQClientException(response.getCode(), response.getRemark());
//    }
    void consumerSendMessageBack(MessageExt msg,
                                 String consumerGroup,
                                 int delayLevel,
                                 long timeoutMillis,
                                 int maxConsumeRetryTimes) throws SQLException;

    void shutdown();

    PullResult pullMessage(PullMessageRequestHeader requestHeader,
                           long timeoutMillis,
                           CommunicationMode communicationMode,
                           PullCallback pullCallback) throws MQBrokerException, SQLException;

    long queryConsumerOffset(QueryConsumerOffsetRequestHeader requestHeader,
                             long timeoutMillis) throws MQBrokerException, SQLException;

    void updateConsumerOffset(UpdateConsumerOffsetRequestHeader requestHeader,
                              long timeoutMillis) throws MQBrokerException, SQLException;

    long searchOffset(String topic, int queueId, long timestamp,
                      long timeoutMillis) throws MQBrokerException, SQLException;

    long getMaxOffset(String topic, int queueId, long timeoutMillis) throws SQLException;

    long getMinOffset(String topic, int queueId, long timeoutMillis) throws SQLException;

    long getEarliestMsgStoretime(String topic, int queueId,
                                 long timeoutMillis) throws SQLException, MQBrokerException;

    Set<MessageQueue> lockBatchMQ(LockBatchRequestBody requestBody, long timeoutMillis);

    Set<MessageQueue> unlockBatchMQ(UnlockBatchRequestBody requestBody, long timeoutMillis, boolean oneway);

    boolean tryLock(String lockId, String lockValue, long timeoutMillis);

    boolean unlock(String lockId, String lockValue, long timeoutMillis);

    void scanNotActiveProducer();

    void scanNotActiveConsumer();

    void setClientConfig(ClientConfig clientConfig);
}
