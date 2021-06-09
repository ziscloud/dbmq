package com.neuronbit.lrdatf.spring.autoconfiguration;

import com.neuronbit.lrdatf.client.consumer.DefaultMQPushConsumer;
import com.neuronbit.lrdatf.client.producer.DefaultMQProducer;
import com.neuronbit.lrdatf.spring.consumer.MessageConsumerScanner;
import com.neuronbit.lrdatf.spring.consumer.MessageListenerConcurrentlyImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@ConditionalOnClass(name = {"javax.sql.DataSource"})
@EnableConfigurationProperties({ConfigProperties.class})
public class LrdatfAutoConfiguration {

    @Bean
    public DefaultMQPushConsumer defaultMQPushConsumer(ConfigProperties properties, DataSource dataSource) {
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(properties.getGroupName());
        consumer.setDataSource(dataSource);

        if (properties.getConsumer() != null) {
            if (properties.getConsumer().getPullInterval() != null) {
                consumer.setPullInterval(properties.getConsumer().getPullInterval().getSeconds());
            }
            if (properties.getConsumer().getMaxReconsumeTimes() != null) {
                consumer.setMaxReconsumeTimes(properties.getConsumer().getMaxReconsumeTimes());
            }
            if (properties.getConsumer().getConsumeFromWhere() != null) {
                consumer.setConsumeFromWhere(properties.getConsumer().getConsumeFromWhere());
            }
            if (properties.getConsumer().getAdjustThreadPoolNumsThreshold() != null) {
                consumer.setAdjustThreadPoolNumsThreshold(properties.getConsumer().getAdjustThreadPoolNumsThreshold());
            }
            if (properties.getConsumer().getAllocateMessageQueueStrategy() != null) {
                consumer.setAllocateMessageQueueStrategy(properties.getConsumer().getAllocateMessageQueueStrategy().getInstance());
            }
            if (properties.getConsumer().getAwaitTerminationMillisWhenShutdown() != null) {
                consumer.setAwaitTerminationMillisWhenShutdown(properties.getConsumer().getAwaitTerminationMillisWhenShutdown().toMillis());
            }
            if (properties.getConsumer().getConsumeConcurrentlyMaxSpan() != null) {
                consumer.setConsumeConcurrentlyMaxSpan(properties.getConsumer().getConsumeConcurrentlyMaxSpan());
            }
            if (properties.getConsumer().getConsumeMessageBatchMaxSize() != null) {
                consumer.setConsumeMessageBatchMaxSize(properties.getConsumer().getConsumeMessageBatchMaxSize());
            }
            if (properties.getConsumer().getConsumeThreadMax() != null) {
                consumer.setConsumeThreadMax(properties.getConsumer().getConsumeThreadMax());
            }
            if (properties.getConsumer().getConsumeThreadMin() != null) {
                consumer.setConsumeThreadMin(properties.getConsumer().getConsumeThreadMin());
            }
            if (properties.getConsumer().getConsumeTimeout() != null) {
                consumer.setConsumeTimeout(properties.getConsumer().getConsumeTimeout().toMinutes());
            }
            if (properties.getConsumer().getConsumeTimestamp() != null) {
                consumer.setConsumeTimestamp(properties.getConsumer().getConsumeTimestamp());
            }
            if (properties.getConsumer().getMessageModel() != null) {
                consumer.setMessageModel(properties.getConsumer().getMessageModel());
            }
            if (properties.getConsumer().getPullBatchSize() != null) {
                consumer.setPullBatchSize(properties.getConsumer().getPullBatchSize());
            }
            if (properties.getConsumer().getPullThresholdForQueue() != null) {
                consumer.setPullThresholdForQueue(properties.getConsumer().getPullThresholdForQueue());
            }
            if (properties.getConsumer().getPullThresholdForTopic() != null) {
                consumer.setPullThresholdForTopic(properties.getConsumer().getPullThresholdForTopic());
            }
            if (properties.getConsumer().getPullThresholdSizeForQueue() != null) {
                consumer.setPullThresholdSizeForQueue(properties.getConsumer().getPullThresholdSizeForQueue());
            }
            if (properties.getConsumer().getPullThresholdSizeForTopic() != null) {
                consumer.setPullThresholdSizeForTopic(properties.getConsumer().getPullThresholdSizeForTopic());
            }
            if (properties.getConsumer().getSuspendCurrentQueueTimeMillis() != null) {
                consumer.setSuspendCurrentQueueTimeMillis(properties.getConsumer().getSuspendCurrentQueueTimeMillis().toMillis());
            }
        }

        if (properties.getClientIp() != null) {
            consumer.setClientIP(properties.getClientIp());
        }
        if (properties.getInstanceName() != null) {
            consumer.setInstanceName(properties.getInstanceName());
        }
        if (properties.getHeartbeatBrokerInterval() != null) {
            consumer.setHeartbeatBrokerInterval((int) properties.getHeartbeatBrokerInterval().getSeconds());
        }
        if (properties.getPersistConsumerOffsetInterval() != null) {
            consumer.setPersistConsumerOffsetInterval((int) properties.getPersistConsumerOffsetInterval().getSeconds());
        }
        if (properties.getPollNameServerInterval() != null) {
            consumer.setPollNameServerInterval((int) properties.getPollNameServerInterval().getSeconds());
        }
        if (properties.getPullTimeDelayMillsWhenException() != null) {
            consumer.setPullTimeDelayMillsWhenException(properties.getPullTimeDelayMillsWhenException().toMillis());
        }

        consumer.registerMessageListener(new MessageListenerConcurrentlyImpl());
        return consumer;
    }

    @Bean
    public MessageConsumerScanner messageConsumerScanner(DefaultMQPushConsumer consumer, DefaultMQProducer producer) {
        return new MessageConsumerScanner(consumer, producer);
    }

    @Bean
    public DefaultMQProducer defaultMQProducer(ConfigProperties properties, DataSource dataSource) {
        final DefaultMQProducer producer = new DefaultMQProducer(properties.getGroupName());
        producer.setDataSource(dataSource);

        if (properties.getClientIp() != null) {
            producer.setClientIP(properties.getClientIp());
        }
        if (properties.getInstanceName() != null) {
            producer.setInstanceName(properties.getInstanceName());
        }
        if (properties.getHeartbeatBrokerInterval() != null) {
            producer.setHeartbeatBrokerInterval((int) properties.getHeartbeatBrokerInterval().getSeconds());
        }
        if (properties.getPersistConsumerOffsetInterval() != null) {
            producer.setPersistConsumerOffsetInterval((int) properties.getPersistConsumerOffsetInterval().getSeconds());
        }
        if (properties.getPollNameServerInterval() != null) {
            producer.setPollNameServerInterval((int) properties.getPollNameServerInterval().getSeconds());
        }
        if (properties.getPullTimeDelayMillsWhenException() != null) {
            producer.setPullTimeDelayMillsWhenException(properties.getPullTimeDelayMillsWhenException().toMillis());
        }

        if (properties.getProducer() != null) {
            if (properties.getProducer().getDefaultTopicQueueNums() != null) {
                producer.setDefaultTopicQueueNums(properties.getProducer().getDefaultTopicQueueNums());
            }
            if (properties.getProducer().getRetryTimesWhenSendFailed() != null) {
                producer.setRetryTimesWhenSendFailed(properties.getProducer().getRetryTimesWhenSendFailed());
            }
            if (properties.getProducer().getSendMsgTimeout() != null) {
                producer.setSendMsgTimeout((int) properties.getProducer().getSendMsgTimeout().getSeconds());
            }
            if (properties.getProducer().getClientCallbackExecutorThreads() != null) {
                producer.setClientCallbackExecutorThreads(properties.getProducer().getClientCallbackExecutorThreads());
            }
        }
        return producer;
    }
}
