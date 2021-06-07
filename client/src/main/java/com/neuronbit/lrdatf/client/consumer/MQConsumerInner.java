package com.neuronbit.lrdatf.client.consumer;

import com.neuronbit.lrdatf.common.consumer.ConsumeFromWhere;
import com.neuronbit.lrdatf.common.message.MessageQueue;
import com.neuronbit.lrdatf.common.protocol.body.ConsumerRunningInfo;
import com.neuronbit.lrdatf.common.protocol.heartbeat.ConsumeType;
import com.neuronbit.lrdatf.common.protocol.heartbeat.MessageModel;
import com.neuronbit.lrdatf.common.protocol.heartbeat.SubscriptionData;

import java.util.Set;

public interface MQConsumerInner {
    String groupName();

    MessageModel messageModel();

    ConsumeType consumeType();

    ConsumeFromWhere consumeFromWhere();

    Set<SubscriptionData> subscriptions();

    void doRebalance();

    void persistConsumerOffset();

    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    boolean isSubscribeTopicNeedUpdate(final String topic);

    boolean isUnitMode();

    ConsumerRunningInfo consumerRunningInfo();
}
