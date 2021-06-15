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

package com.neuronbit.lrdatf.spring;

import com.neuronbit.lrdatf.client.producer.DefaultMQProducer;
import com.neuronbit.lrdatf.client.producer.SendResult;
import com.neuronbit.lrdatf.client.producer.SendStatus;
import com.neuronbit.lrdatf.common.message.Message;
import com.neuronbit.lrdatf.exception.MQClientException;
import com.neuronbit.lrdatf.exception.RemotingTooMuchRequestException;
import com.neuronbit.lrdatf.listener.TestMessageListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AutoConfigurationApplication.class)
public class AutoConfigurationTest {
    @Autowired
    private DefaultMQProducer mqProducer;
    @Autowired
    private TestMessageListener listener;

    @Autowired
    private DataSource dataSource;

    private static boolean dataLoaded = false;

    @Before
    public void setup() throws SQLException {
        if (!dataLoaded) {
            try (Connection con = dataSource.getConnection()) {
                ScriptUtils.executeSqlScript(con, new ClassPathResource("init_schema.sql"));
                dataLoaded = true;
            }
        }
    }

    @Test
    public void loadContext() {

    }

    @Test
    public void testMessageConsume() throws InterruptedException, MQClientException, RemotingTooMuchRequestException {
        final CountDownLatch countDownLatch = new CountDownLatch(100);
        listener.setCountDownLatch(countDownLatch);
        listener.resetCount();

        for (int i = 0; i < 100; i++) {
            final SendResult sendResult = mqProducer.send(new Message("TestTopic", "{\"msg\":\"test message x" + i + "\"}"));
            assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        }

        countDownLatch.await();

        assertThat(listener.getCount()).isEqualTo(100);
    }
}
