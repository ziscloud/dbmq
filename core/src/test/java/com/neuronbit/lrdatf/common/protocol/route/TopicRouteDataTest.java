/*
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
 */

package com.neuronbit.lrdatf.common.protocol.route;


import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class TopicRouteDataTest {
    @Test
    public void testTopicRouteDataClone() throws Exception {

        TopicRouteData topicRouteData = new TopicRouteData();

        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker-a");
        queueData.setPerm(6);
        queueData.setReadQueueNums(8);
        queueData.setWriteQueueNums(8);
        queueData.setTopicSynFlag(0);

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        queueDataList.add(queueData);

        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "192.168.0.47:10911");
        brokerAddrs.put(1L, "192.168.0.47:10921");

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        topicRouteData.setQueueDatas(queueDataList);

        assertThat(topicRouteData.cloneTopicRouteData()).isEqualTo(topicRouteData);

    }
}
