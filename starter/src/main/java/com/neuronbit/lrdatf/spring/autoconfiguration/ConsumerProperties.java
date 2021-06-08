package com.neuronbit.lrdatf.spring.autoconfiguration;

import com.neuronbit.lrdatf.common.consumer.ConsumeFromWhere;
import com.neuronbit.lrdatf.common.protocol.heartbeat.MessageModel;
import com.neuronbit.lrdatf.spring.convert.DurationUnit;
import com.neuronbit.lrdatf.spring.enums.AllocateMessageQueueMode;
import lombok.Data;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Data
public class ConsumerProperties {
    @DurationUnit(ChronoUnit.SECONDS)
    private Duration pullInterval;
    private Integer maxReconsumeTimes;
    private ConsumeFromWhere consumeFromWhere;
    private Integer adjustThreadPoolNumsThreshold;
    private AllocateMessageQueueMode allocateMessageQueueStrategy;
    @DurationUnit(ChronoUnit.MILLIS)
    private Duration awaitTerminationMillisWhenShutdown;
    private Integer consumeConcurrentlyMaxSpan;
    private Integer consumeMessageBatchMaxSize;
    private Integer consumeThreadMax;
    private Integer consumeThreadMin;
    @DurationUnit(ChronoUnit.MINUTES)
    private Duration consumeTimeout;
    private String consumeTimestamp;
    private MessageModel messageModel;
    private Integer pullBatchSize;
    private Integer pullThresholdForQueue;
    private Integer pullThresholdForTopic;
    private Integer pullThresholdSizeForQueue;
    private Integer pullThresholdSizeForTopic;
    @DurationUnit(ChronoUnit.MILLIS)
    private Duration suspendCurrentQueueTimeMillis;
}
