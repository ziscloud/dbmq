/*
 *
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
 *
 */

package com.neuronbit.dbmq.client.producer;

import com.neuronbit.dbmq.client.ClientConfig;
import com.neuronbit.dbmq.client.Validators;
import com.neuronbit.dbmq.client.impl.producer.DefaultMQProducerImpl;
import com.neuronbit.dbmq.common.constant.LoggerName;
import com.neuronbit.dbmq.common.message.Message;
import com.neuronbit.dbmq.common.topic.TopicValidator;
import com.neuronbit.dbmq.exception.MQClientException;
import com.neuronbit.dbmq.exception.RemotingTooMuchRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMQProducer extends ClientConfig implements MQProducer {
    private final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);

    private final String producerGroup;
    /**
     * Timeout for sending messages.
     */
    private int sendMsgTimeout = 3000;

    /**
     * Maximum allowed message size in bytes.
     */
    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    /**
     * Number of queues to create per default topic.
     */
    private volatile int defaultTopicQueueNums = 4;

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    /**
     * Maximum number of retry to perform internally before claiming sending failure in synchronous mode. </p>
     * <p>
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendFailed = 2;

    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;

    public DefaultMQProducer(String producerGroup) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this);
    }

    public void start() throws MQClientException {
        this.defaultMQProducerImpl.start();
    }

    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
    }

    public SendResult send(Message msg) throws MQClientException, InterruptedException, RemotingTooMuchRequestException {
        Validators.checkMessage(msg, this);
        return this.defaultMQProducerImpl.send(msg);
    }

    public SendResult send(Message msg, long timeout) throws MQClientException, InterruptedException, RemotingTooMuchRequestException {
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return this.defaultMQProducerImpl;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public String getCreateTopicKey() {
        //Just for testing or demo program
        return TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }
}
