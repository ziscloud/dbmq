package com.neuronbit.lrdatf.client.comsumer;

import com.neuronbit.lrdatf.common.message.MessageQueue;

import java.util.Set;

public interface TopicMessageQueueChangeListener {
    void onChanged(String topic, Set<MessageQueue> messageQueues);
}
