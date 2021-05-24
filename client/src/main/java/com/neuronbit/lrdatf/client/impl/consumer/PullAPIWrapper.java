/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.neuronbit.lrdatf.client.impl.consumer;

import com.neuronbit.lrdatf.client.comsumer.MQClientInstance;
import com.neuronbit.lrdatf.client.comsumer.PullCallback;
import com.neuronbit.lrdatf.client.comsumer.PullResult;
import com.neuronbit.lrdatf.client.comsumer.PullStatus;
import com.neuronbit.lrdatf.client.impl.CommunicationMode;
import com.neuronbit.lrdatf.common.MixAll;
import com.neuronbit.lrdatf.common.constant.LoggerName;
import com.neuronbit.lrdatf.common.message.MessageAccessor;
import com.neuronbit.lrdatf.common.message.MessageConst;
import com.neuronbit.lrdatf.common.message.MessageExt;
import com.neuronbit.lrdatf.common.message.MessageQueue;
import com.neuronbit.lrdatf.common.protocol.header.PullMessageRequestHeader;
import com.neuronbit.lrdatf.common.protocol.heartbeat.SubscriptionData;
import com.neuronbit.lrdatf.exception.MQBrokerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PullAPIWrapper {
    private final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.currentTimeMillis());
//    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
                                        final SubscriptionData subscriptionData) {

        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            List<MessageExt> msgList = pullResult.getMsgFoundList();

            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

//            if (this.hasHook()) {
//                FilterMessageContext filterMessageContext = new FilterMessageContext();
//                filterMessageContext.setUnitMode(unitMode);
//                filterMessageContext.setMsgList(msgListFilterAgain);
//                this.executeHook(filterMessageContext);
//            }

            for (MessageExt msg : msgListFilterAgain) {
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                        Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                        Long.toString(pullResult.getMaxOffset()));
            }

            pullResult.setMsgFoundList(msgListFilterAgain);
        }
        return pullResult;
    }

//    public boolean hasHook() {
//        return !this.filterMessageHookList.isEmpty();
//    }
//
//    public void executeHook(final FilterMessageContext context) {
//        if (!this.filterMessageHookList.isEmpty()) {
//            for (FilterMessageHook hook : this.filterMessageHookList) {
//                try {
//                    hook.filterMessage(context);
//                } catch (Throwable e) {
//                    log.error("execute hook error. hookName={}", hook.hookName());
//                }
//            }
//        }
//    }

    public PullResult pullKernelImpl(
            final MessageQueue mq,
            final String subExpression,
            final String expressionType,
            final long subVersion,
            final long offset,
            final int maxNums,
            final int sysFlag,
            final long commitOffset,
            final long brokerSuspendMaxTimeMillis,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final PullCallback pullCallback
    ) throws InterruptedException, SQLException, MQBrokerException {

        int sysFlagInner = sysFlag;

        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setConsumerGroup(this.consumerGroup);
        requestHeader.setTopic(mq.getTopic());
        requestHeader.setQueueId(mq.getQueueId());
        requestHeader.setQueueOffset(offset);
        requestHeader.setMaxMsgNums(maxNums);
        requestHeader.setSysFlag(sysFlagInner);
        requestHeader.setCommitOffset(commitOffset);
        requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
        requestHeader.setSubscription(subExpression);
        requestHeader.setSubVersion(subVersion);
        requestHeader.setExpressionType(expressionType);

        PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                requestHeader,
                timeoutMillis,
                communicationMode,
                pullCallback);

        return pullResult;

    }

//    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
//        this.filterMessageHookList = filterMessageHookList;
//    }

}
