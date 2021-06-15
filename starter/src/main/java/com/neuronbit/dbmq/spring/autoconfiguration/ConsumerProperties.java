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

package com.neuronbit.dbmq.spring.autoconfiguration;

import com.neuronbit.dbmq.common.consumer.ConsumeFromWhere;
import com.neuronbit.dbmq.common.protocol.heartbeat.MessageModel;
import com.neuronbit.dbmq.spring.enums.AllocateMessageQueueMode;
import lombok.Data;
import org.springframework.boot.convert.DurationUnit;

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
