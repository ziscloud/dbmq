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

package com.neuronbit.lrdatf.client.impl.producer;

import com.neuronbit.lrdatf.client.Validators;
import com.neuronbit.lrdatf.client.common.ClientErrorCode;
import com.neuronbit.lrdatf.client.impl.CommunicationMode;
import com.neuronbit.lrdatf.client.impl.MQClientManager;
import com.neuronbit.lrdatf.client.impl.factory.MQClientInstance;
import com.neuronbit.lrdatf.client.producer.DefaultMQProducer;
import com.neuronbit.lrdatf.client.producer.SendCallback;
import com.neuronbit.lrdatf.client.producer.SendResult;
import com.neuronbit.lrdatf.client.producer.SendStatus;
import com.neuronbit.lrdatf.common.MixAll;
import com.neuronbit.lrdatf.common.ServiceState;
import com.neuronbit.lrdatf.common.help.FAQUrl;
import com.neuronbit.lrdatf.common.message.*;
import com.neuronbit.lrdatf.common.protocol.NamespaceUtil;
import com.neuronbit.lrdatf.common.protocol.header.SendMessageRequestHeader;
import com.neuronbit.lrdatf.exception.MQClientException;
import com.neuronbit.lrdatf.exception.RemotingTooMuchRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMQProducerImpl implements MQProducerInner {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Random random = new Random();
    private final DefaultMQProducer defaultMQProducer;
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private final ExecutorService defaultAsyncSenderExecutor;
    private ExecutorService asyncSenderExecutor;
    private final Timer timer = new Timer("RequestHouseKeepingService", true);

    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();

    public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer) {
        this.defaultMQProducer = defaultMQProducer;

        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(50000),
                new ThreadFactory() {
                    private final AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncSenderExecutor_" + this.threadIndex.incrementAndGet());
                    }
                });
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    public void start(final boolean startFactory) throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;

                this.checkConfig();

                if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    this.defaultMQProducer.changeInstanceNameToPID();
                }

                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer);

                boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

                if (startFactory) {
                    mQClientFactory.start();
                }

                log.info("the producer [{}] start OK.", this.defaultMQProducer.getProducerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }

        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

        if (null == this.defaultMQProducer.getProducerGroup()) {
            throw new MQClientException("producerGroup is null", null);
        }

        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
                    null);
        }
    }

    public void shutdown() {
        this.shutdown(true);
    }

    public void shutdown(final boolean shutdownFactory) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
                this.defaultAsyncSenderExecutor.shutdown();
                if (shutdownFactory) {
                    this.mQClientFactory.shutdown();
                }
                this.timer.cancel();
                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    public SendResult send(
            Message msg) throws MQClientException, InterruptedException, RemotingTooMuchRequestException {
        return send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, long timeout) throws MQClientException, InterruptedException, RemotingTooMuchRequestException {
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
    }

//    public void send(Message msg,
//                     SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
//        send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
//    }
//
//    /**
//     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
//     * provided in next version
//     *
//     * @param msg
//     * @param sendCallback
//     * @param timeout      the <code>sendCallback</code> will be invoked at most time
//     * @throws RejectedExecutionException
//     */
//    @Deprecated
//    public void send(final Message msg, final SendCallback sendCallback, final long timeout)
//            throws MQClientException, RemotingException, InterruptedException {
//        final long beginStartTime = System.currentTimeMillis();
//        ExecutorService executor = this.getAsyncSenderExecutor();
//        try {
//            executor.submit(new Runnable() {
//                @Override
//                public void run() {
//                    long costTime = System.currentTimeMillis() - beginStartTime;
//                    if (timeout > costTime) {
//                        try {
//                            sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
//                        } catch (Exception e) {
//                            sendCallback.onException(e);
//                        }
//                    } else {
//                        sendCallback.onException(
//                                new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
//                    }
//                }
//
//            });
//        } catch (RejectedExecutionException e) {
//            throw new MQClientException("executor rejected ", e);
//        }
//    }

    public ExecutorService getAsyncSenderExecutor() {
        return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
    }

    private SendResult sendDefaultImpl(Message msg, final CommunicationMode communicationMode, final SendCallback sendCallback, final long timeout)
            throws MQClientException, InterruptedException, RemotingTooMuchRequestException {
        this.checkServiceState();

        Validators.checkMessage(msg, this.defaultMQProducer);

        final long invokeID = random.nextLong();
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        long endTimestamp;

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            boolean callTimeout = false;
            MessageQueue mq = null;
            Exception exception = null;
            SendResult sendResult = null;
            int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
            int times = 0;
            for (; times < timesTotal; times++) {
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo);
                if (mqSelected != null) {
                    mq = mqSelected;
                    try {
                        beginTimestampPrev = System.currentTimeMillis();
                        if (times > 0) {
                            //Reset topic with namespace during resend.
                            msg.setTopic(this.defaultMQProducer.withNamespace(msg.getTopic()));
                        }
                        long costTime = beginTimestampPrev - beginTimestampFirst;
                        if (timeout < costTime) {
                            callTimeout = true;
                            break;
                        }

                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                        endTimestamp = System.currentTimeMillis();
                        //this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        switch (communicationMode) {
                            case ASYNC:
                                return null;
                            case ONEWAY:
                                return null;
                            case SYNC:
                                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                    //if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                    continue;
                                    //}
                                }

                                return sendResult;
                            default:
                                break;
                        }
                    } catch (MQClientException e) {
                        endTimestamp = System.currentTimeMillis();
                        //this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (InterruptedException e) {
                        endTimestamp = System.currentTimeMillis();
                        //this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        log.warn(String.format("sendKernelImpl exception, throw exception, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());

                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        throw e;
                    }
                } else {
                    break;
                }
            }

            if (sendResult != null) {
                return sendResult;
            }

            String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s",
                    times,
                    System.currentTimeMillis() - beginTimestampFirst,
                    msg.getTopic());

            info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

            MQClientException mqClientException = new MQClientException(info, exception);
            if (callTimeout) {
                throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
            }

            if (exception != null) {
                mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
            }

            throw mqClientException;
        }

        throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
                null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
    }


    private SendResult sendKernelImpl(final Message msg,
                                      final MessageQueue mq,
                                      final CommunicationMode communicationMode,
                                      final SendCallback sendCallback,
                                      final TopicPublishInfo topicPublishInfo,
                                      final long timeout) throws MQClientException, InterruptedException, RemotingTooMuchRequestException {
        long beginStartTime = System.currentTimeMillis();
        //SendMessageContext context = null;
        String prevBody = msg.getBody();
        try {
            boolean topicWithNamespace = false;
            if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());
                topicWithNamespace = true;
            }

            int sysFlag = 0;
            boolean msgBodyCompressed = false;

//            if (hasCheckForbiddenHook()) {
//                CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
//                checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
//                checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
//                checkForbiddenContext.setCommunicationMode(communicationMode);
//                checkForbiddenContext.setBrokerAddr(brokerAddr);
//                checkForbiddenContext.setMessage(msg);
//                checkForbiddenContext.setMq(mq);
//                checkForbiddenContext.setUnitMode(this.isUnitMode());
//                this.executeCheckForbiddenHook(checkForbiddenContext);
//            }
//
//            if (this.hasSendMessageHook()) {
//                context = new SendMessageContext();
//                context.setProducer(this);
//                context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
//                context.setCommunicationMode(communicationMode);
//                context.setBornHost(this.defaultMQProducer.getClientIP());
//                context.setBrokerAddr(brokerAddr);
//                context.setMessage(msg);
//                context.setMq(mq);
//                context.setNamespace(this.defaultMQProducer.getNamespace());
//                String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
//                if (isTrans != null && isTrans.equals("true")) {
//                    context.setMsgType(MessageType.Trans_Msg_Half);
//                }
//
//                if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
//                    context.setMsgType(MessageType.Delay_Msg);
//                }
//                this.executeSendMessageHookBefore(context);
//            }

            SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
            requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
            requestHeader.setTopic(msg.getTopic());
            requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
            requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setSysFlag(sysFlag);
            requestHeader.setBornTimestamp(System.currentTimeMillis());
            requestHeader.setBornHost(this.defaultMQProducer.getClientIP());
            requestHeader.setFlag(msg.getFlag());
            requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
            requestHeader.setReconsumeTimes(0);
            requestHeader.setUnitMode(this.isUnitMode());
            if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                if (reconsumeTimes != null) {
                    requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                    MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                }

                String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                if (maxReconsumeTimes != null) {
                    requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                    MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                }
            }

            SendResult sendResult = null;
            switch (communicationMode) {
                case ASYNC:
//                    Message tmpMessage = msg;
//                    boolean messageCloned = false;
//                    if (msgBodyCompressed) {
//                        //If msg body was compressed, msgbody should be reset using prevBody.
//                        //Clone new message using commpressed message body and recover origin massage.
//                        //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
//                        tmpMessage = MessageAccessor.cloneMessage(msg);
//                        messageCloned = true;
//                        msg.setBody(prevBody);
//                    }
//
//                    if (topicWithNamespace) {
//                        if (!messageCloned) {
//                            tmpMessage = MessageAccessor.cloneMessage(msg);
//                            messageCloned = true;
//                        }
//                        msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
//                    }
//
//                    long costTimeAsync = System.currentTimeMillis() - beginStartTime;
//                    if (timeout < costTimeAsync) {
//                        throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
//                    }
//                    sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
//                            brokerAddr,
//                            mq.getBrokerName(),
//                            tmpMessage,
//                            requestHeader,
//                            timeout - costTimeAsync,
//                            communicationMode,
//                            sendCallback,
//                            topicPublishInfo,
//                            this.mQClientFactory,
//                            this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(),
//                            context,
//                            this);
//                    break;
                case ONEWAY:
                case SYNC:
                    long costTimeSync = System.currentTimeMillis() - beginStartTime;
                    if (timeout < costTimeSync) {
                        throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                    }
                    sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
//                            brokerAddr,
//                            mq.getBrokerName(),
                            msg,
                            requestHeader,
                            timeout - costTimeSync,
                            communicationMode,
                            //context,
                            this);
                    break;
                default:
                    assert false;
                    break;
            }

//            if (this.hasSendMessageHook()) {
//                context.setSendResult(sendResult);
//                this.executeSendMessageHookAfter(context);
//            }

            return sendResult;
        } catch (InterruptedException e) {
//            if (this.hasSendMessageHook()) {
//                context.setException(e);
//                this.executeSendMessageHookAfter(context);
//            }
            throw e;
        } finally {
            msg.setBody(prevBody);
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
        }

        //throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    private void checkServiceState() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }

    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            // TODO: 2021/5/10 from database
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }

        if (topicPublishInfo.ok()) {
            return topicPublishInfo;
        } else {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo) {
        return tpInfo.selectOneMessageQueue();
    }

    public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
        this.asyncSenderExecutor = asyncSenderExecutor;
    }

    @Override
    public void updateTopicPublishInfo(String topic, TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev.toString());
            }
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        Set<String> topicList = new HashSet<String>();
        for (String key : this.topicPublishInfoTable.keySet()) {
            topicList.add(key);
        }

        return topicList;
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQProducer.isUnitMode();
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }
}
