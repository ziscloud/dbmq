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

package com.neuronbit.dbmq.spring.enums;

import com.neuronbit.dbmq.client.consumer.AllocateMessageQueueStrategy;
import com.neuronbit.dbmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum AllocateMessageQueueMode {
    AVG(AllocateMessageQueueAveragely.class);

    private final Class<? extends AllocateMessageQueueStrategy> clazz;

    AllocateMessageQueueMode(Class<? extends AllocateMessageQueueStrategy> clazz) {
        this.clazz = clazz;
    }

    public AllocateMessageQueueStrategy getInstance() {
        try {
            return this.clazz.newInstance();
        } catch (Exception e) {
            log.error("can not create instance of {}", clazz.getName(), e);
            return null;
        }
    }
}
