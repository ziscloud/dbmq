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

package com.neuronbit.dbmq.client;

import com.neuronbit.dbmq.client.impl.factory.MQClientInstance;
import com.neuronbit.dbmq.common.constant.LoggerName;
import com.neuronbit.dbmq.remoting.common.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientHousekeepingService extends ServiceThread {
    private static long waitInterval =
            Long.parseLong(System.getProperty(
                    "rocketmq.client.rebalance.waitInterval", "20000"));
    private final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);
    private final MQClientInstance mqClientFactory;

    public ClientHousekeepingService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public String getServiceName() {
        return ClientHousekeepingService.class.getSimpleName();
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            this.waitForRunning(waitInterval);
            this.mqClientFactory.scanException();
        }

        log.info(this.getServiceName() + " service end");
    }
}
