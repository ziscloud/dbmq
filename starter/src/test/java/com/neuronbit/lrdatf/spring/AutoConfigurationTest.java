package com.neuronbit.lrdatf.spring;

import com.neuronbit.lrdatf.client.producer.MQProducer;
import com.neuronbit.lrdatf.client.producer.SendResult;
import com.neuronbit.lrdatf.client.producer.SendStatus;
import com.neuronbit.lrdatf.common.message.Message;
import com.neuronbit.lrdatf.exception.MQClientException;
import com.neuronbit.lrdatf.exception.RemotingTooMuchRequestException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AutoConfigurationApplication.class)
public class AutoConfigurationTest {
    @Autowired
    private MQProducer mqProducer;

    @Test
    public void loadContext() {

    }

    @Test
    public void testMessageProduce() throws InterruptedException, MQClientException, RemotingTooMuchRequestException {
        final SendResult sendResult = mqProducer.send(new Message("TestTopic", "this is message body"));
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
    }
}
