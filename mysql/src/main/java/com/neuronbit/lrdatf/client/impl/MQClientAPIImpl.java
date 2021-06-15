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
import com.neuronbit.lrdatf.client.consumer.PullStatus;
import com.neuronbit.lrdatf.client.impl.factory.MQClientInstance;
import com.neuronbit.lrdatf.client.impl.producer.DefaultMQProducerImpl;
import com.neuronbit.lrdatf.client.impl.producer.TopicPublishInfo;
import com.neuronbit.lrdatf.client.producer.SendCallback;
import com.neuronbit.lrdatf.client.producer.SendResult;
import com.neuronbit.lrdatf.client.producer.SendStatus;
import com.neuronbit.lrdatf.common.MixAll;
import com.neuronbit.lrdatf.common.constant.LoggerName;
import com.neuronbit.lrdatf.common.message.Message;
import com.neuronbit.lrdatf.common.message.MessageClientIDSetter;
import com.neuronbit.lrdatf.common.message.MessageExt;
import com.neuronbit.lrdatf.common.message.MessageQueue;
import com.neuronbit.lrdatf.common.protocol.ResponseCode;
import com.neuronbit.lrdatf.common.protocol.body.LockBatchRequestBody;
import com.neuronbit.lrdatf.common.protocol.body.UnlockBatchRequestBody;
import com.neuronbit.lrdatf.common.protocol.header.PullMessageRequestHeader;
import com.neuronbit.lrdatf.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.neuronbit.lrdatf.common.protocol.header.SendMessageRequestHeader;
import com.neuronbit.lrdatf.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.neuronbit.lrdatf.common.protocol.heartbeat.ConsumerData;
import com.neuronbit.lrdatf.common.protocol.heartbeat.HeartbeatData;
import com.neuronbit.lrdatf.common.protocol.heartbeat.ProducerData;
import com.neuronbit.lrdatf.common.protocol.heartbeat.SubscriptionData;
import com.neuronbit.lrdatf.common.protocol.route.QueueData;
import com.neuronbit.lrdatf.common.protocol.route.TopicRouteData;
import com.neuronbit.lrdatf.common.utils.JSON;
import com.neuronbit.lrdatf.exception.MQBrokerException;
import com.neuronbit.lrdatf.exception.MQClientException;
import com.neuronbit.lrdatf.exception.RemotingTooMuchRequestException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

import static com.neuronbit.lrdatf.remoting.common.RemotingUtil.string2InetAddress;

public class MQClientAPIImpl implements MQClientAPI {
    private final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);
    protected final static int DLQ_NUMS_PER_GROUP = 1;
    private final Random random = new Random(System.currentTimeMillis());
    private ClientConfig clientConfig;
    private DataSource pool;

    public MQClientAPIImpl() {
    }

//    public void createTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
//                            final long timeoutMillis)
//            throws InterruptedException, MQClientException {
//        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
//        requestHeader.setTopic(topicConfig.getTopicName());
//        requestHeader.setDefaultTopic(defaultTopic);
//        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
//        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
//        requestHeader.setPerm(topicConfig.getPerm());
//        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
//        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
//        requestHeader.setOrder(topicConfig.isOrder());
//
//        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
//
//        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
//                request, timeoutMillis);
//        assert response != null;
//        switch (response.getCode()) {
//            case ResponseCode.SUCCESS: {
//                return;
//            }
//            default:
//                break;
//        }
//
//        throw new MQClientException(response.getCode(), response.getRemark());
//    }

    @Override
    public void start() {
        pool = clientConfig.getDataSource();
    }

    @Override
    public int sendHeartbeat(final HeartbeatData heartbeatData, final long timeoutMillis) throws SQLException {
        int timeout = (int) Duration.ofMillis(timeoutMillis).getSeconds();
        Connection connection = pool.getConnection();
        PreparedStatement insertConsumerStmt = null;
        PreparedStatement deleteProducerStmt = null;
        PreparedStatement insertProducerStmt = null;
        PreparedStatement deleteConsumerStmt = null;

        try {
            connection.setAutoCommit(false);

            deleteProducerStmt = connection.prepareStatement("delete from producer_data where client_id=?");
            deleteProducerStmt.setString(1, heartbeatData.getClientID());
            deleteProducerStmt.setQueryTimeout(timeout);
            deleteProducerStmt.execute();

            if (!heartbeatData.getProducerDataSet().isEmpty()) {
                insertProducerStmt = connection.prepareStatement("insert into" +
                        " producer_data(client_id, group_name, beat_timestamp) values (?, ?, unix_timestamp())");
                for (ProducerData producerData : heartbeatData.getProducerDataSet()) {
                    insertProducerStmt.setString(1, heartbeatData.getClientID());
                    insertProducerStmt.setString(2, producerData.getGroupName());
                    insertProducerStmt.setQueryTimeout(timeout);
                    insertProducerStmt.addBatch();
                }
                insertProducerStmt.setQueryTimeout(timeout);
                insertProducerStmt.executeBatch();
            }

            deleteConsumerStmt = connection.prepareStatement("delete from consumer_data where client_id=?");
            deleteConsumerStmt.setString(1, heartbeatData.getClientID());
            deleteConsumerStmt.setQueryTimeout(timeout);
            deleteConsumerStmt.execute();

            if (!heartbeatData.getConsumerDataSet().isEmpty()) {
                insertConsumerStmt = connection.prepareStatement("insert into " +
                        "consumer_data(client_id, group_name, consume_type, message_model, consume_from, topic, sub_version, beat_timestamp)" +
                        " values(?,?,?,?,?,?,?,unix_timestamp())");
                for (ConsumerData consumerData : heartbeatData.getConsumerDataSet()) {
                    for (SubscriptionData subscriptionData : consumerData.getSubscriptionDataSet()) {
                        insertConsumerStmt.setString(1, heartbeatData.getClientID());
                        insertConsumerStmt.setString(2, consumerData.getGroupName());
                        insertConsumerStmt.setString(3, consumerData.getConsumeType().toString());
                        insertConsumerStmt.setString(4, consumerData.getMessageModel().toString());
                        insertConsumerStmt.setString(5, consumerData.getConsumeFromWhere().toString());
                        insertConsumerStmt.setString(6, subscriptionData.getTopic());
                        insertConsumerStmt.setLong(7, subscriptionData.getSubVersion());
                        insertConsumerStmt.addBatch();
                    }
                }
                insertConsumerStmt.setQueryTimeout(timeout);
                insertConsumerStmt.executeBatch();
            }

            connection.commit();
        } catch (Exception e) {
            log.error("send heartbeat failed", e);
            connection.rollback();
        } finally {
            if (insertConsumerStmt != null) {
                insertConsumerStmt.close();
            }
            if (insertProducerStmt != null) {
                insertProducerStmt.close();
            }
            if (deleteConsumerStmt != null) {
                deleteConsumerStmt.close();
            }
            if (deleteProducerStmt != null) {
                deleteProducerStmt.close();
            }
            connection.close();
        }

        return 1;
    }

    @Override
    public void unregisterClient(final String clientID,
                                 final String producerGroup,
                                 final String consumerGroup,
                                 final long timeoutMillis) throws InterruptedException, SQLException {
        final int timeout = (int) Duration.ofMillis(timeoutMillis).getSeconds();
        Connection connection = pool.getConnection();
        PreparedStatement deleteConsumerStmt = null;
        PreparedStatement deleteProducerStmt = null;
        try {
            connection.setAutoCommit(false);
            if (StringUtils.isNotBlank(producerGroup)) {
                deleteProducerStmt = connection.prepareStatement("delete from producer_data where client_id=? and group_name=?");
                deleteProducerStmt.setString(1, clientID);
                deleteProducerStmt.setString(2, producerGroup);
                deleteProducerStmt.setQueryTimeout(timeout);
                deleteProducerStmt.execute();
            }

            if (StringUtils.isNotBlank(consumerGroup)) {
                deleteConsumerStmt = connection.prepareStatement("delete from consumer_data where client_id=? and group_name=?");
                deleteConsumerStmt.setString(1, clientID);
                deleteConsumerStmt.setString(2, producerGroup);
                deleteConsumerStmt.setQueryTimeout(timeout);
                deleteConsumerStmt.execute();
            }

            connection.commit();
        } catch (Exception e) {
            connection.rollback();
        } finally {
            if (deleteConsumerStmt != null) {
                deleteConsumerStmt.close();
            }
            if (deleteProducerStmt != null) {
                deleteProducerStmt.close();
            }
            connection.close();
        }
    }

    @Override
    public SendResult sendMessage(final Message msg,
                                  final SendMessageRequestHeader requestHeader,
                                  final long timeoutMillis,
                                  final CommunicationMode communicationMode,
//            final SendMessageContext context,
                                  final DefaultMQProducerImpl producer) throws InterruptedException, RemotingTooMuchRequestException, MQClientException {
        return sendMessage(msg, requestHeader, timeoutMillis, communicationMode, null, null, null, 0, /*context, */producer);
    }

    @Override
    public SendResult sendMessage(final Message msg,
                                  final SendMessageRequestHeader requestHeader,
                                  final long timeoutMillis,
                                  final CommunicationMode communicationMode,
                                  final SendCallback sendCallback,
                                  final TopicPublishInfo topicPublishInfo,
                                  final MQClientInstance instance,
                                  final int retryTimesWhenSendFailed,
                                  //final SendMessageContext context,
                                  final DefaultMQProducerImpl producer) throws RemotingTooMuchRequestException, MQClientException {
        long beginStartTime = System.currentTimeMillis();


        switch (communicationMode) {
            case ONEWAY:
//                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
//                return null;
            case ASYNC:
//                final AtomicInteger times = new AtomicInteger();
//                long costTimeAsync = System.currentTimeMillis() - beginStartTime;
//                if (timeoutMillis < costTimeAsync) {
//                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
//                }
//                this.sendMessageAsync(/*addr, brokerName,*/ msg, timeoutMillis - costTimeAsync, request, sendCallback, topicPublishInfo, instance,
//                        retryTimesWhenSendFailed, times, /*context,*/ producer);
//                return null;
            case SYNC:
                long costTimeSync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeSync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                try {
                    return this.sendMessageSync(/*addr, brokerName,*/ msg, (int) (timeoutMillis - costTimeSync), requestHeader);
                } catch (SQLException e) {
                    throw new MQClientException("send message sync failed", e);
                }
            default:
                assert false;
                break;
        }

        return null;
    }

    private SendResult sendMessageSync(final Message msg,
                                       final long timeoutMillis,
                                       final SendMessageRequestHeader requestHeader) throws SQLException {

        final Connection connection = pool.getConnection();
        final Integer queueId = requestHeader.getQueueId();
        PreparedStatement statement = null;
        try {
            connection.setAutoCommit(false);

            statement = connection.prepareStatement("insert into message_" + msg.getTopic() + "_" + queueId
                    + "(topic, `keys`, tags, body, queue_id, born_timestamp, born_host, msg_id," +
                    " properties, max_recon_times, recon_times, producer_group, store_timestamp)" +
                    " values(?,?,?,?,?,?,?,?,?,?,?,?, unix_timestamp())");
            statement.setString(1, msg.getTopic());
            statement.setString(2, msg.getKeys());
            statement.setString(3, msg.getTags());
            statement.setString(4, msg.getBody());
            statement.setInt(5, queueId);
            statement.setLong(6, requestHeader.getBornTimestamp());
            statement.setString(7, requestHeader.getBornHost());
            statement.setString(8, MessageClientIDSetter.createUniqID());
            statement.setString(9, JSON.toJSONString(msg.getProperties()));
            statement.setInt(10, requestHeader.getMaxReconsumeTimes() == null ? 0 : requestHeader.getMaxReconsumeTimes());
            statement.setInt(11, requestHeader.getReconsumeTimes());
            statement.setString(12, requestHeader.getProducerGroup());
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());
            statement.execute();

            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            if (statement != null) {
                statement.close();
            }
            connection.close();
        }

        final SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        return sendResult;
    }

//    private void sendMessageAsync(
////            final String addr,
////            final String brokerName,
//            final Message msg,
//            final long timeoutMillis,
//            final RemotingCommand request,
//            final SendCallback sendCallback,
//            final TopicPublishInfo topicPublishInfo,
//            final MQClientInstance instance,
//            final int retryTimesWhenSendFailed,
//            final AtomicInteger times,
//            final SendMessageContext context,
//            final DefaultMQProducerImpl producer
//    ) throws InterruptedException, RemotingException {
//        final long beginStartTime = System.currentTimeMillis();
//        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
//            @Override
//            public void operationComplete(ResponseFuture responseFuture) {
//                long cost = System.currentTimeMillis() - beginStartTime;
//                RemotingCommand response = responseFuture.getResponseCommand();
//                if (null == sendCallback && response != null) {
//
//                    try {
//                        SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
//                        if (context != null && sendResult != null) {
//                            context.setSendResult(sendResult);
//                            context.getProducer().executeSendMessageHookAfter(context);
//                        }
//                    } catch (Throwable e) {
//                    }
//
//                    producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);
//                    return;
//                }
//
//                if (response != null) {
//                    try {
//                        SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
//                        assert sendResult != null;
//                        if (context != null) {
//                            context.setSendResult(sendResult);
//                            context.getProducer().executeSendMessageHookAfter(context);
//                        }
//
//                        try {
//                            sendCallback.onSuccess(sendResult);
//                        } catch (Throwable e) {
//                        }
//
//                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);
//                    } catch (Exception e) {
//                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), true);
//                        onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
//                                retryTimesWhenSendFailed, times, e, context, false, producer);
//                    }
//                } else {
//                    producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), true);
//                    if (!responseFuture.isSendRequestOK()) {
//                        MQClientException ex = new MQClientException("send request failed", responseFuture.getCause());
//                        onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
//                                retryTimesWhenSendFailed, times, ex, context, true, producer);
//                    } else if (responseFuture.isTimeout()) {
//                        MQClientException ex = new MQClientException("wait response timeout " + responseFuture.getTimeoutMillis() + "ms",
//                                responseFuture.getCause());
//                        onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
//                                retryTimesWhenSendFailed, times, ex, context, true, producer);
//                    } else {
//                        MQClientException ex = new MQClientException("unknow reseaon", responseFuture.getCause());
//                        onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
//                                retryTimesWhenSendFailed, times, ex, context, true, producer);
//                    }
//                }
//            }
//        });
//    }

//    public TopicStatsTable getTopicStatsInfo(final String addr, final String topic,
//                                             final long timeoutMillis) throws InterruptedException,
//            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
//        GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
//        requestHeader.setTopic(topic);
//
//        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);
//
//        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
//                request, timeoutMillis);
//        switch (response.getCode()) {
//            case ResponseCode.SUCCESS: {
//                TopicStatsTable topicStatsTable = TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
//                return topicStatsTable;
//            }
//            default:
//                break;
//        }
//
//        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
//    }

//    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final long timeoutMillis)
//            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
//            MQBrokerException {
//        return getConsumeStats(addr, consumerGroup, null, timeoutMillis);
//    }

//    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final String topic,
//                                        final long timeoutMillis)
//            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
//            MQBrokerException {
//        GetConsumeStatsRequestHeader requestHeader = new GetConsumeStatsRequestHeader();
//        requestHeader.setConsumerGroup(consumerGroup);
//        requestHeader.setTopic(topic);
//
//        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);
//
//        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
//                request, timeoutMillis);
//        switch (response.getCode()) {
//            case ResponseCode.SUCCESS: {
//                ConsumeStats consumeStats = ConsumeStats.decode(response.getBody(), ConsumeStats.class);
//                return consumeStats;
//            }
//            default:
//                break;
//        }
//
//        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
//    }

    @Override
    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws MQClientException, InterruptedException, SQLException {

        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, false);
    }

    @Override
    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws MQClientException, InterruptedException, SQLException {

        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, true);
    }

    @Override
    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis,
                                                          boolean allowTopicNotExist) throws SQLException {
        Connection connection = pool.getConnection();
        final TopicRouteData data = new TopicRouteData();
        data.setQueueDatas(new ArrayList<>());
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement("select * from topic_config where topic=? and status = 'ACTIVE'");
            statement.setString(1, topic);
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());
            final ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                final QueueData queueData = new QueueData();
                queueData.setReadQueueNums(resultSet.getInt("queue_nums"));
                queueData.setWriteQueueNums(resultSet.getInt("queue_nums"));
                queueData.setPerm(resultSet.getInt("perm"));
                data.getQueueDatas().add(queueData);
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            connection.close();
        }

        return data;
    }

    @Override
    public void checkClientInBroker(final String consumerGroup,
                                    final String clientId, final SubscriptionData subscriptionData,
                                    final long timeoutMillis) throws MQClientException, SQLException {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("select * from consumer_data " +
                    "where client_id = ? and group_name=? and topic=? and sub_version=?");
            statement.setString(1, clientId);
            statement.setString(2, consumerGroup);
            statement.setString(3, subscriptionData.getTopic());
            statement.setLong(4, subscriptionData.getSubVersion());
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());
            final ResultSet resultSet = statement.executeQuery();
            if (!resultSet.next()) {
                throw new MQClientException(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, "check client failed");
            }
        } finally {
            if (null != statement) {
                statement.close();
            }
        }
    }

    @Override
    public List<String> getConsumerIdListByGroup(final String consumerGroup,
                                                 final long timeoutMillis) throws SQLException {
        List<String> result = new ArrayList<>();
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("select distinct client_id from consumer_data where group_name=?");
            statement.setString(1, consumerGroup);
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());
            final ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString("client_id"));
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }

        return result;
    }

//    public TopicList getTopicsByCluster(final String cluster, final long timeoutMillis)
//            throws RemotingException, MQClientException, InterruptedException {
//        GetTopicsByClusterRequestHeader requestHeader = new GetTopicsByClusterRequestHeader();
//        requestHeader.setCluster(cluster);
//        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, requestHeader);
//
//        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
//        assert response != null;
//        switch (response.getCode()) {
//            case ResponseCode.SUCCESS: {
//                byte[] body = response.getBody();
//                if (body != null) {
//                    TopicList topicList = TopicList.decode(body, TopicList.class);
//                    return topicList;
//                }
//            }
//            default:
//                break;
//        }
//
//        throw new MQClientException(response.getCode(), response.getRemark());
//    }
//
//    public void registerMessageFilterClass(final String addr,
//                                           final String consumerGroup,
//                                           final String topic,
//                                           final String className,
//                                           final int classCRC,
//                                           final byte[] classBody,
//                                           final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
//            InterruptedException, MQBrokerException {
//        RegisterMessageFilterClassRequestHeader requestHeader = new RegisterMessageFilterClassRequestHeader();
//        requestHeader.setConsumerGroup(consumerGroup);
//        requestHeader.setClassName(className);
//        requestHeader.setTopic(topic);
//        requestHeader.setClassCRC(classCRC);
//
//        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_MESSAGE_FILTER_CLASS, requestHeader);
//        request.setBody(classBody);
//        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
//        switch (response.getCode()) {
//            case ResponseCode.SUCCESS: {
//                return;
//            }
//            default:
//                break;
//        }
//
//        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
//    }

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
    @Override
    public void consumerSendMessageBack(final MessageExt msg,
                                        final String consumerGroup,
                                        final int delayLevel,
                                        final long timeoutMillis,
                                        final int maxConsumeRetryTimes) throws SQLException {
        final Connection connection = pool.getConnection();
        PreparedStatement statement = null;
        try {
            connection.setAutoCommit(false);

            String newTopic = MixAll.getRetryTopic(consumerGroup);
            int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % 1/*subscriptionGroupConfig.getRetryQueueNums()*/;

            if (msg.getReconsumeTimes() >= maxConsumeRetryTimes) {
                newTopic = MixAll.getDLQTopic(consumerGroup);
                queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;
            }

            statement = connection.prepareStatement("insert into message_" + newTopic + "_" + queueIdInt
                    + "(topic, `keys`, tags, body, queue_id, born_timestamp, born_host, msg_id," +
                    " properties, max_recon_times, recon_times, consumer_group, store_timestamp)" +
                    " values(?,?,?,?,?,?,?,?,?,?,?,?, unix_timestamp())");
            statement.setString(1, msg.getTopic());
            statement.setString(2, msg.getKeys());
            statement.setString(3, msg.getTags());
            statement.setString(4, msg.getBody());
            statement.setInt(5, msg.getQueueId());
            statement.setLong(6, msg.getBornTimestamp());
            statement.setString(7, msg.getBornHostString());
            statement.setString(8, msg.getMsgId());
            statement.setString(9, JSON.toJSONString(msg.getProperties()));
            statement.setInt(10, maxConsumeRetryTimes);
            statement.setInt(11, msg.getReconsumeTimes() + 1);
            statement.setString(12, consumerGroup);
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());
            statement.execute();

            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            if (statement != null) {
                statement.close();
            }
            connection.close();
        }
    }

    @Override
    public void shutdown() {
    }

    @Override
    public PullResult pullMessage(final PullMessageRequestHeader requestHeader,
                                  final long timeoutMillis,
                                  final CommunicationMode communicationMode,
                                  final PullCallback pullCallback) throws MQBrokerException, SQLException {
        switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                this.pullMessageAsync(requestHeader, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.pullMessageSync(requestHeader, timeoutMillis);
            default:
                assert false;
                break;
        }

        return null;
    }

    private void pullMessageAsync(final PullMessageRequestHeader request,
                                  final long timeoutMillis,
                                  final PullCallback pullCallback) throws SQLException {
        final int timeout = (int) Duration.ofMillis(timeoutMillis).getSeconds();
        PreparedStatement messageStmt = null;
        PreparedStatement idStmt = null;
        try (Connection connection = pool.getConnection()) {
            final Integer queueId = request.getQueueId();

            messageStmt = connection.prepareStatement("select * " +
                    "from message_" + request.getTopic() + "_" + queueId + " where id >= ? limit ?");
            messageStmt.setLong(1, request.getCommitOffset());
            messageStmt.setInt(2, request.getMaxMsgNums());
            messageStmt.setQueryTimeout(timeout);

            idStmt = connection.prepareStatement(
                    "select min(id), max(id) from message_" + request.getTopic() + "_" + queueId);
            idStmt.setQueryTimeout(timeout);

            PullResult pullResult = this.processPullResponse(messageStmt.executeQuery(), idStmt.executeQuery());
            pullCallback.onSuccess(pullResult);
        } catch (Exception e) {
            pullCallback.onException(e);
        } finally {
            if (idStmt != null) {
                idStmt.close();
            }
            if (messageStmt != null) {
                messageStmt.close();
            }
        }
    }

    @Override
    public long queryConsumerOffset(final QueryConsumerOffsetRequestHeader requestHeader,
                                    final long timeoutMillis) throws MQBrokerException, SQLException {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("select offset_value from consumer_offset " +
                    "where topic=? and queue_id=? and group_name=?");
            statement.setString(1, requestHeader.getTopic());
            statement.setInt(2, requestHeader.getQueueId());
            statement.setString(3, requestHeader.getConsumerGroup());
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());

            long offset = -1;
            final ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                offset = resultSet.getLong("offset_value");
            }
            if (offset == -1) {
                throw new MQBrokerException(ResponseCode.QUERY_NOT_FOUND, "offset no found");
            }

            statement.close();
            return offset;
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    @Override
    public void updateConsumerOffset(final UpdateConsumerOffsetRequestHeader requestHeader,
                                     final long timeoutMillis) throws MQBrokerException, SQLException {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("insert into consumer_offset(group_name," +
                    " topic, queue_id, offset_value, update_timestamp) values(?,?,?,?, unix_timestamp()) on duplicate key" +
                    " update offset_value = ?, update_timestamp=unix_timestamp()");
            statement.setString(1, requestHeader.getConsumerGroup());
            statement.setString(2, requestHeader.getTopic());
            statement.setInt(3, requestHeader.getQueueId());
            statement.setLong(4, requestHeader.getCommitOffset());
            statement.setLong(5, requestHeader.getCommitOffset());
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());

            final int affectedRows = statement.executeUpdate();
            statement.close();
            if (2 != affectedRows && 1 != affectedRows) {
                throw new MQBrokerException(ResponseCode.SYSTEM_ERROR, "offset no found");
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    @Override
    public long searchOffset(final String topic, final int queueId, final long timestamp,
                             final long timeoutMillis) throws MQBrokerException, SQLException {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("select id from message_" + topic + "_" + queueId
                    + " where topic=? and queue_id=? and born_timestamp>? order by id asc limit 1");
            statement.setString(1, topic);
            statement.setInt(2, queueId);
            statement.setLong(3, timestamp);
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());

            long offset = -1;
            final ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                offset = resultSet.getLong("id");
            }
            if (offset == -1) {
                throw new MQBrokerException(ResponseCode.QUERY_NOT_FOUND, "offset no found");
            }
            return offset;
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    @Override
    public long getMaxOffset(final String topic, final int queueId, final long timeoutMillis) throws SQLException {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("select max(id) " +
                    "from message_" + topic + "_" + queueId + " where topic=? and queue_id=?");
            statement.setString(1, topic);
            statement.setInt(2, queueId);
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());

            long offset = 0;
            final ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                offset = resultSet.getLong(1);
            }
            return offset;
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    @Override
    public long getMinOffset(final String topic, final int queueId, final long timeoutMillis) throws SQLException {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("select min(id) " +
                    "from message_" + topic + "_" + queueId + " where topic=? and queue_id=?");
            statement.setString(1, topic);
            statement.setInt(2, queueId);
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());

            long offset = -1;
            final ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                offset = resultSet.getLong(1);
            }

            return offset;
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    @Override
    public long getEarliestMsgStoretime(final String topic, final int queueId,
                                        final long timeoutMillis) throws SQLException, MQBrokerException {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("select store_timestamp " +
                    "from message_" + topic + "_" + queueId + " where topic=? and queue_id=? order by id asc limit 1");
            statement.setString(1, topic);
            statement.setInt(2, queueId);
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());

            long offset = -1;
            final ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                offset = resultSet.getLong(0);
            }
            if (offset == -1) {
                throw new MQBrokerException(ResponseCode.QUERY_NOT_FOUND, "offset no found");
            }
            return offset;
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private PullResult pullMessageSync(
            final PullMessageRequestHeader request,
            final long timeoutMillis
    ) throws SQLException {
        final int timeout = (int) Duration.ofMillis(timeoutMillis).getSeconds();
        PreparedStatement msgStmt = null;
        PreparedStatement idStmt = null;
        try (Connection connection = pool.getConnection()) {
            final Integer queueId = request.getQueueId();
            msgStmt = connection.prepareStatement("select * " +
                    "from message_" + request.getTopic() + "_" + queueId + " where id >= ? limit ?");
            msgStmt.setLong(1, request.getCommitOffset());
            msgStmt.setInt(2, request.getMaxMsgNums());
            msgStmt.setQueryTimeout(timeout);

            idStmt = connection.prepareStatement("select min(id), max(id) " +
                    "from message_" + request.getTopic() + "_" + queueId);
            idStmt.setQueryTimeout(timeout);

            return this.processPullResponse(msgStmt.executeQuery(), idStmt.executeQuery());
        } finally {
            if (idStmt != null) {
                idStmt.close();
            }
            if (msgStmt != null) {
                msgStmt.close();
            }
        }
    }

    private PullResult processPullResponse(
            final ResultSet response, ResultSet resultSet) throws SQLException {
        PullStatus pullStatus = PullStatus.NO_NEW_MSG;
        // TODO: 2021/5/19 优化
//        switch (response.getCode()) {
//            case ResponseCode.SUCCESS:
//                pullStatus = PullStatus.FOUND;
//                break;
//            case ResponseCode.PULL_NOT_FOUND:
//                pullStatus = PullStatus.NO_NEW_MSG;
//                break;
//            case ResponseCode.PULL_RETRY_IMMEDIATELY:
//                pullStatus = PullStatus.NO_MATCHED_MSG;
//                break;
//            case ResponseCode.PULL_OFFSET_MOVED:
//                pullStatus = PullStatus.OFFSET_ILLEGAL;
//                break;
//
//            default:
//                throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
//        }
        List<MessageExt> messageExts = new ArrayList<>();
        int nextBeginOffset = 0;
        while (response.next()) {
            MessageExt messageExt = new MessageExt();
            messageExt.setBornHost(string2InetAddress(response.getString("born_host")));
            messageExt.setBornTimestamp(response.getLong("born_timestamp"));
            messageExt.setBody(response.getString("body"));
            messageExt.setMsgId(response.getString("msg_id"));
            messageExt.setQueueId(response.getInt("queue_id"));
            messageExt.setReconsumeTimes(response.getInt("recon_times"));
            messageExt.setKeys(response.getString("keys"));
            messageExt.setTags(response.getString("tags"));
            messageExt.setTopic(response.getString("topic"));

            final int id = response.getInt("id");
            messageExt.setQueueOffset(id);
            nextBeginOffset = id;

            messageExts.add(messageExt);
        }

        long minOffset = 0;
        long maxOffset = 0;
        while (resultSet.next()) {
            minOffset = resultSet.getLong(1);
            maxOffset = resultSet.getLong(2);
        }

        if (!messageExts.isEmpty()) {
            pullStatus = PullStatus.FOUND;
        }
        return new PullResult(pullStatus, nextBeginOffset, minOffset, maxOffset, messageExts);
    }

    @Override
    public Set<MessageQueue> lockBatchMQ(final LockBatchRequestBody requestBody, final long timeoutMillis) {
        final int timeout = (int) Duration.ofMillis(timeoutMillis).getSeconds();
        final String clientId = requestBody.getClientId();
        final String consumerGroup = requestBody.getConsumerGroup();
        final Set<MessageQueue> mqSet = requestBody.getMqSet();
        Set<MessageQueue> lockOKMQSet = new HashSet<>(mqSet.size());

        try (Connection connection = pool.getConnection()) {
            PreparedStatement statement;
            for (MessageQueue messageQueue : mqSet) {
                String lockId = consumerGroup + "_" + messageQueue.getQueueId();

                statement = connection.prepareStatement("update lock_table " +
                        " set lock_value=?, lock_timestamp=unix_timestamp()" +
                        " where lock_id=? and (lock_value is null or lock_value=? or lock_timestamp < (unix_timestamp()- ?))");
                statement.setString(1, clientId);
                statement.setString(2, lockId);
                statement.setString(3, clientId);
                // TODO: 2021/5/21 read lock timeout from config
                statement.setLong(4, Duration.ofMinutes(60).getSeconds());
                statement.setQueryTimeout(timeout);
                try {
                    final int affectedRows = statement.executeUpdate();
                    if (affectedRows == 1) {
                        lockOKMQSet.add(messageQueue);
                        continue;
                    }
                } catch (Exception e) {
                    log.debug("try to lock {} by update failed", lockId, e);
                } finally {
                    if (null != statement) {
                        statement.close();
                    }
                }

                statement = connection.prepareStatement("insert into lock_table (lock_id, lock_value, lock_timestamp)" +
                        " values(?,?, unix_timestamp())");
                statement.setString(1, lockId);
                statement.setString(2, clientId);
                statement.setQueryTimeout(timeout);
                try {
                    final int affectedRows = statement.executeUpdate();
                    if (affectedRows == 1) {
                        lockOKMQSet.add(messageQueue);
                        continue;
                    }
                } catch (Exception e) {
                    log.debug("try to lock {} by insert failed", lockId, e);
                } finally {
                    if (null != statement) {
                        statement.close();
                    }
                }
            }
        } catch (SQLException e) {
            log.error("try to lockBatchMQ {} failed", requestBody, e);
        }

        return lockOKMQSet;
    }

    @Override
    public Set<MessageQueue> unlockBatchMQ(final UnlockBatchRequestBody requestBody, final long timeoutMillis, final boolean oneway) {
        final int timeout = (int) Duration.ofMillis(timeoutMillis).getSeconds();
        final String clientId = requestBody.getClientId();
        final String consumerGroup = requestBody.getConsumerGroup();
        final Set<MessageQueue> mqSet = requestBody.getMqSet();
        Set<MessageQueue> lockOKMQSet = new HashSet<>(mqSet.size());

        try (Connection connection = pool.getConnection()) {
            PreparedStatement statement;
            for (MessageQueue messageQueue : mqSet) {
                String lockId = consumerGroup + "_" + messageQueue.getQueueId();
                statement = connection.prepareStatement("update lock_table " +
                        " set lock_value=null, lock_timestamp=0" +
                        " where lock_id=? and lock_value=?");
                statement.setString(1, lockId);
                statement.setString(2, clientId);
                statement.setQueryTimeout(timeout);
                try {
                    final int affectedRows = statement.executeUpdate();
                    if (affectedRows == 1) {
                        lockOKMQSet.add(messageQueue);
                    }
                    continue;
                } catch (Exception e) {
                    log.debug("try to unlock {} by update failed", lockId, e);
                } finally {
                    if (null != statement) {
                        statement.close();
                    }
                }
            }
        } catch (SQLException e) {
            log.error("try to unlockBatchMQ {} failed", requestBody, e);
        }

        return lockOKMQSet;
    }

    @Override
    public boolean tryLock(String lockId, String lockValue, long timeoutMillis) {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("update lock_table " +
                    " set lock_value=?, lock_timestamp=unix_timestamp()" +
                    " where lock_id=? and (lock_value is null or lock_value=? or lock_timestamp < (unix_timestamp()- ?))");
            statement.setString(1, lockValue);
            statement.setString(2, lockId);
            statement.setString(3, lockValue);
            // TODO: 2021/5/21 read lock timeout from config
            statement.setLong(4, Duration.ofMinutes(5).getSeconds());
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());
            final int affectedRows = statement.executeUpdate();
            return affectedRows == 1;
        } catch (SQLException e) {
            log.error("try to lock {} failed", lockId, e);
            return false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //ignore
                }
            }
        }
    }

    @Override
    public boolean unlock(String lockId, String lockValue, long timeoutMillis) {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("update lock_table " +
                    " set lock_value=NULL, lock_timestamp=0" +
                    " where lock_id=? and lock_value=?");
            statement.setString(1, lockId);
            statement.setString(2, lockValue);
            statement.setQueryTimeout((int) Duration.ofMillis(timeoutMillis).getSeconds());
            final int affectedRows = statement.executeUpdate();
            return affectedRows == 1;
        } catch (SQLException e) {
            log.error("try to unlock {} failed", lockId, e);
            return false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //ignore
                }
            }
        }
    }

    @Override
    public void scanNotActiveProducer() {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("delete from producer_data " +
                    " where beat_timestamp<(unix_timestamp()- ?)");
            statement.setLong(1, Duration.ofMinutes(5).getSeconds());
            statement.setQueryTimeout(1);
            final int affectedRows = statement.executeUpdate();
            log.warn("removed {} NOT ACTIVE producer", affectedRows);
        } catch (SQLException e) {
            log.error("try to removed NOT ACTIVE producer failed", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //ignore
                }
            }
        }
    }

    @Override
    public void scanNotActiveConsumer() {
        PreparedStatement statement = null;
        try (Connection connection = pool.getConnection()) {
            statement = connection.prepareStatement("delete from consumer_data " +
                    " where beat_timestamp<(unix_timestamp()- ?)");
            statement.setLong(1, Duration.ofMinutes(5).getSeconds());
            statement.setQueryTimeout(1);
            final int affectedRows = statement.executeUpdate();
            log.warn("removed {} NOT ACTIVE consumer", affectedRows);
        } catch (SQLException e) {
            log.error("try to removed NOT ACTIVE consumer failed", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //ignore
                }
            }
        }
    }

    @Override
    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

}
