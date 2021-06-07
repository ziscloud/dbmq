package com.neuronbit.lrdatf.spring.consumer;

import com.neuronbit.lrdatf.common.message.MessageExt;
import com.neuronbit.lrdatf.common.utils.JSON;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class MessageConsumerAdaptor {
    private final Object listener;
    private final Method method;

    public MessageConsumerAdaptor(Object listener, Method method) {
        this.listener = listener;
        this.method = method;
    }

    public void consume(List<MessageExt> msgs) throws InvocationTargetException, IllegalAccessException {
        final Class<?>[] parameterTypes = method.getParameterTypes();
        for (MessageExt msg : msgs) {
            final String body = msg.getBody();
            method.invoke(listener, JSON.parseObject(body, parameterTypes[0]));
        }

    }
}
