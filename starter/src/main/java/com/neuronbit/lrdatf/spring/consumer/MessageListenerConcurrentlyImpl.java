package com.neuronbit.lrdatf.spring.consumer;

import com.neuronbit.lrdatf.client.consumer.listener.ConsumeConcurrentlyContext;
import com.neuronbit.lrdatf.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.neuronbit.lrdatf.client.consumer.listener.MessageListenerConcurrently;
import com.neuronbit.lrdatf.common.message.MessageExt;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MessageListenerConcurrentlyImpl implements MessageListenerConcurrently {
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

    public void register(String topic, MessageConsumerAdaptor messageConsumerAdaptor) {
        handlers.put(topic, messageConsumerAdaptor);
    }
}
