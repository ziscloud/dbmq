package com.neuronbit.lrdatf.spring.autoconfiguration;

import com.neuronbit.lrdatf.spring.convert.DurationUnit;
import lombok.Data;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Data
public class ProducerProperties {
    private Integer defaultTopicQueueNums;
    private Integer retryTimesWhenSendFailed;
    @DurationUnit(ChronoUnit.SECONDS)
    private Duration sendMsgTimeout;
    private Integer clientCallbackExecutorThreads;
}
