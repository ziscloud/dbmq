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

package com.neuronbit.dbmq.spring.consumer;

import com.neuronbit.dbmq.client.consumer.listener.ConsumeOrderlyContext;
import com.neuronbit.dbmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.neuronbit.dbmq.client.consumer.listener.MessageListenerOrderly;
import com.neuronbit.dbmq.common.message.MessageExt;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MessageListenerOrderlyImpl implements MessageListenerOrderly, MessageConsumerRegistry {
    private ConcurrentHashMap<String, MessageConsumerAdaptor> handlers = new ConcurrentHashMap<>();

    @Override
    public void register(String topic, MessageConsumerAdaptor messageConsumerAdaptor) {
        handlers.put(topic, messageConsumerAdaptor);
    }

    /**
     * It is not recommend to throw exception,rather than returning ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
     * if consumption failure
     *
     * @param msgs    msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @param context
     * @return The consume status
     */
    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        final String topic = context.getMessageQueue().getTopic();
        final MessageConsumerAdaptor adaptor = handlers.get(topic);
        if (adaptor != null) {
            try {
                adaptor.consume(msgs);
            } catch (Exception e) {
                log.error("consume message failed", e);
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
            return ConsumeOrderlyStatus.SUCCESS;
        } else {
            log.error("");
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
    }
}
