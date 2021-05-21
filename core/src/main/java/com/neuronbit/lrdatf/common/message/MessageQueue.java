package com.neuronbit.lrdatf.common.message;

import java.io.Serializable;

public class MessageQueue implements Comparable<MessageQueue>, Serializable {
    private static final long serialVersionUID = 6191200464116433425L;
    private String topic;
    // TODO: 2021/5/17 如果为了考虑单个DB的负载过大的情况，那么可以将topic分布到DB上面去，
    //  这样的话，这里的brokerName就有意义了，而且在produce的时候，需要查询出具体的broker，也就是DB的地址
    //  这样的话，需要用一个DB作为NameServer，来保存topic和Broker之间的分布关系，
    //  如果需要这么复杂的话，还不如直接上RocketMQ好了
    // private String brokerName;
    private int queueId;

    public MessageQueue() {

    }

    public MessageQueue(String topic, int queueId) {
        this.topic = topic;
        //this.brokerName = brokerName;
        this.queueId = queueId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

//    public String getBrokerName() {
//        return brokerName;
//    }

//    public void setBrokerName(String brokerName) {
//        this.brokerName = brokerName;
//    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
//        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + queueId;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MessageQueue other = (MessageQueue) obj;
//        if (brokerName == null) {
//            if (other.brokerName != null)
//                return false;
//        } else if (!brokerName.equals(other.brokerName))
//            return false;
        if (queueId != other.queueId)
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MessageQueue [topic=" + topic /*+ ", brokerName=" + brokerName*/ + ", queueId=" + queueId + "]";
    }

    @Override
    public int compareTo(MessageQueue o) {
        {
            int result = this.topic.compareTo(o.topic);
            if (result != 0) {
                return result;
            }
        }

//        {
//            int result = this.brokerName.compareTo(o.brokerName);
//            if (result != 0) {
//                return result;
//            }
//        }

        return this.queueId - o.queueId;
    }
}
