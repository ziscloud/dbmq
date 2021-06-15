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
