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

import com.neuronbit.dbmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.neuronbit.dbmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.neuronbit.dbmq.client.consumer.listener.MessageListenerConcurrently;
import com.neuronbit.dbmq.common.message.MessageExt;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MessageListenerConcurrentlyImpl implements MessageListenerConcurrently, MessageConsumerRegistry {
    private ConcurrentHashMap<String, MessageConsumerAdaptor> handlers = new ConcurrentHashMap<>();

    /**
     * It is not recommend to throw exception,rather than returning ConsumeConcurrentlyStatus.RECONSUME_LATER if
     * consumption failure
     *
     * @param msgs    msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @param context
     * @return The consume status
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        final String topic = context.getMessageQueue().getTopic();
        final MessageConsumerAdaptor adaptor = handlers.get(topic);
        if (adaptor != null) {
            try {
                adaptor.consume(msgs);
            } catch (Exception e) {
                log.error("consume message failed", e);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } else {
            log.error("");
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    @Override
    public void register(String topic, MessageConsumerAdaptor messageConsumerAdaptor) {
        handlers.put(topic, messageConsumerAdaptor);
    }
}
