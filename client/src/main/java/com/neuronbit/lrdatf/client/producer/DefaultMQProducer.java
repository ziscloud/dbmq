package com.neuronbit.lrdatf.client.producer;

import com.neuronbit.lrdatf.client.ClientConfig;
import com.neuronbit.lrdatf.client.Validators;
import com.neuronbit.lrdatf.client.impl.producer.DefaultMQProducerImpl;
import com.neuronbit.lrdatf.common.constant.LoggerName;
import com.neuronbit.lrdatf.common.message.Message;
import com.neuronbit.lrdatf.common.topic.TopicValidator;
import com.neuronbit.lrdatf.exception.MQClientException;
import com.neuronbit.lrdatf.exception.RemotingTooMuchRequestException;
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
//
//    /**
//     * Send message to broker asynchronously. </p>
//     * <p>
//     * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed. </p>
//     * <p>
//     * Similar to {@link #send(Message)}, internal implementation would potentially retry up to {@link
//     * #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication and
//     * application developers are the one to resolve this potential issue.
//     *
//     * @param msg          Message to send.
//     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
//     * @throws MQClientException    if there is any client error.
//     * @throws RemotingException    if there is any network-tier error.
//     * @throws InterruptedException if the sending thread is interrupted.
//     */
////    @Override
////    public void send(Message msg,
////                     SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
////        msg.setTopic(withNamespace(msg.getTopic()));
////        this.defaultMQProducerImpl.send(msg, sendCallback);
////    }
//
//    /**
//     * Same to {@link #send(Message, SendCallback)} with send timeout specified in addition.
//     *
//     * @param msg          message to send.
//     * @param sendCallback Callback to execute.
//     * @param timeout      send timeout.
//     * @throws MQClientException    if there is any client error.
//     * @throws RemotingException    if there is any network-tier error.
//     * @throws InterruptedException if the sending thread is interrupted.
//     */
////    @Override
////    public void send(Message msg, SendCallback sendCallback, long timeout)
////            throws MQClientException, RemotingException, InterruptedException {
////        msg.setTopic(withNamespace(msg.getTopic()));
////        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
////    }

    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return this.defaultMQProducerImpl;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public String getCreateTopicKey() {
        /**
         * Just for testing or demo program
         */
        return TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }
}
