package com.neuronbit.lrdatf.spring.autoconfiguration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@ConfigurationProperties(prefix = "lrdatf")
@Data
public class ConfigProperties {
    private String groupName;
    private String clientIp;
    private String instanceName;

    @DurationUnit(ChronoUnit.SECONDS)
    private Duration heartbeatBrokerInterval;

    @DurationUnit(ChronoUnit.SECONDS)
    private Duration persistConsumerOffsetInterval;

    @DurationUnit(ChronoUnit.SECONDS)
    private Duration pollNameServerInterval;

    @DurationUnit(ChronoUnit.MILLIS)
    private Duration pullTimeDelayMillsWhenException;

    private ConsumerProperties consumer;
    private ProducerProperties producer;
}
