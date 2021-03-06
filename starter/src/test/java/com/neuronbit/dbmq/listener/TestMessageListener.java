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

package com.neuronbit.dbmq.listener;

import com.neuronbit.dbmq.spring.annotation.MessageConsumer;
import com.neuronbit.dbmq.spring.annotation.MessageTopic;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@MessageConsumer
public class TestMessageListener {
    private volatile AtomicInteger count = new AtomicInteger(0);
    private CountDownLatch countDownLatch;

    @MessageTopic(name = "TestTopic")
    public void consume(TestMessage msg) {
        log.info("get message:{}, count:{}", msg, count.incrementAndGet());

        if (countDownLatch != null) {
            countDownLatch.countDown();
        }
    }

    public int getCount() {
        return count.get();
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void resetCount() {
        count.set(0);
    }
}
