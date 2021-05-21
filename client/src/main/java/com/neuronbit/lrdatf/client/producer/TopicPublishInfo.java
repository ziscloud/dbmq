package com.neuronbit.lrdatf.client.producer;

import com.neuronbit.lrdatf.client.common.ThreadLocalIndex;
import com.neuronbit.lrdatf.common.message.MessageQueue;
import com.neuronbit.lrdatf.common.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.List;

public class TopicPublishInfo {
    private List<MessageQueue> messageQueueList = new ArrayList<>();
    private final ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private TopicRouteData topicRouteData;

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
