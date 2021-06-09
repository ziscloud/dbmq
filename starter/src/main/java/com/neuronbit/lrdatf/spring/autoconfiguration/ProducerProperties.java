package com.neuronbit.lrdatf.spring.autoconfiguration;

import lombok.Data;
import org.springframework.boot.convert.DurationUnit;

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
