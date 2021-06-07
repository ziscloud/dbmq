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
package com.neuronbit.lrdatf.client.impl;

//@RunWith(MockitoJUnitRunner.class)
public class MQClientAPIImplTest {
//    private MQClientAPI mqClientAPI = new MQClientAPIImpl();
//    @Mock
//    private DefaultMQProducerImpl defaultMQProducerImpl;
//    @Mock
//    private RemotingClient remotingClient;
//
//    private String brokerAddr = "127.0.0.1";
//    private String brokerName = "DefaultBroker";
//    private static String group = "FooBarGroup";
//    private static String topic = "FooBar";
//    private Message msg = new Message("FooBar", "");
//
//    @Before
//    public void init() throws Exception {
//        Field field = MQClientAPIImpl.class.getDeclaredField("remotingClient");
//        field.setAccessible(true);
//        field.set(mqClientAPI, remotingClient);
//    }
//
//    @Test
//    public void testSendMessageOneWay_Success() throws InterruptedException {
//        Mockito.doNothing().when(remotingClient).invokeOneway(ArgumentMatchers.anyString(), ArgumentMatchers.any(RemotingCommand.class), ArgumentMatchers.anyLong());
//        SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
//                3 * 1000, CommunicationMode.ONEWAY, new SendMessageContext(), defaultMQProducerImpl);
//        Assertions.assertThat(sendResult).isNull();
//    }
//
//    @Test
//    public void testSendMessageSync_Success() throws InterruptedException, RemotingException, MQBrokerException {
//        Mockito.doAnswer(new Answer() {
//            @Override
//            public Object answer(InvocationOnMock mock) throws Throwable {
//                RemotingCommand request = mock.getArgument(1);
//                return createSuccessResponse(request);
//            }
//        }).when(remotingClient).invokeSync(ArgumentMatchers.anyString(), ArgumentMatchers.any(RemotingCommand.class), ArgumentMatchers.anyLong());
//
//        SendMessageRequestHeader requestHeader = createSendMessageRequestHeader();
//
//        SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, requestHeader,
//                3 * 1000, CommunicationMode.SYNC, new SendMessageContext(), defaultMQProducerImpl);
//
//        Assertions.assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
//        Assertions.assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
//        Assertions.assertThat(sendResult.getQueueOffset()).isEqualTo(123L);
//        Assertions.assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(1);
//    }
//
//    @Test
//    public void testSendMessageSync_WithException() throws InterruptedException, RemotingException, MQBrokerException {
//        Mockito.doAnswer(new Answer() {
//            @Override
//            public Object answer(InvocationOnMock mock) throws Throwable {
//                RemotingCommand request = mock.getArgument(1);
//                RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
//                response.setCode(ResponseCode.SYSTEM_ERROR);
//                response.setOpaque(request.getOpaque());
//                response.setRemark("Broker is broken.");
//                return response;
//            }
//        }).when(remotingClient).invokeSync(ArgumentMatchers.anyString(), ArgumentMatchers.any(RemotingCommand.class), ArgumentMatchers.anyLong());
//
//        SendMessageRequestHeader requestHeader = createSendMessageRequestHeader();
//
//        try {
//            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, requestHeader,
//                    3 * 1000, CommunicationMode.SYNC, new SendMessageContext(), defaultMQProducerImpl);
//            Fail.failBecauseExceptionWasNotThrown(MQBrokerException.class);
//        } catch (MQBrokerException e) {
//            assertThat(e).hasMessageContaining("Broker is broken.");
//        }
//    }
//
//    @Test
//    public void testSendMessageAsync_Success() throws RemotingException, InterruptedException, MQBrokerException {
//        Mockito.doNothing().when(remotingClient).invokeAsync(ArgumentMatchers.anyString(), ArgumentMatchers.any(RemotingCommand.class), ArgumentMatchers.anyLong(), ArgumentMatchers.any(InvokeCallback.class));
//        SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
//                3 * 1000, CommunicationMode.ASYNC, new SendMessageContext(), defaultMQProducerImpl);
//        Assertions.assertThat(sendResult).isNull();
//
//        Mockito.doAnswer(new Answer() {
//            @Override
//            public Object answer(InvocationOnMock mock) throws Throwable {
//                InvokeCallback callback = mock.getArgument(3);
//                RemotingCommand request = mock.getArgument(1);
//                ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
//                responseFuture.setResponseCommand(createSuccessResponse(request));
//                callback.operationComplete(responseFuture);
//                return null;
//            }
//        }).when(remotingClient).invokeAsync(ArgumentMatchers.anyString(), ArgumentMatchers.any(RemotingCommand.class), ArgumentMatchers.anyLong(), ArgumentMatchers.any(InvokeCallback.class));
//        SendMessageContext sendMessageContext = new SendMessageContext();
//        sendMessageContext.setProducer(new DefaultMQProducerImpl(new DefaultMQProducer()));
//        mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(), 3 * 1000, CommunicationMode.ASYNC,
//                new SendCallback() {
//                    @Override
//                    public void onSuccess(SendResult sendResult) {
//                        Assertions.assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
//                        Assertions.assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
//                        Assertions.assertThat(sendResult.getQueueOffset()).isEqualTo(123L);
//                        Assertions.assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(1);
//                    }
//
//                    @Override
//                    public void onException(Throwable e) {
//                    }
//                },
//                null, null, 0, sendMessageContext, defaultMQProducerImpl);
//    }
//
//    @Test
//    public void testSendMessageAsync_WithException() throws RemotingException, InterruptedException, MQBrokerException {
//        doThrow(new RemotingTimeoutException("Remoting Exception in Test")).when(remotingClient)
//                .invokeAsync(ArgumentMatchers.anyString(), ArgumentMatchers.any(RemotingCommand.class), ArgumentMatchers.anyLong(), ArgumentMatchers.any(InvokeCallback.class));
//        try {
//            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
//                    3 * 1000, CommunicationMode.ASYNC, new SendMessageContext(), defaultMQProducerImpl);
//            Fail.failBecauseExceptionWasNotThrown(RemotingException.class);
//        } catch (RemotingException e) {
//            assertThat(e).hasMessage("Remoting Exception in Test");
//        }
//
//        Mockito.doThrow(new InterruptedException("Interrupted Exception in Test")).when(remotingClient)
//                .invokeAsync(ArgumentMatchers.anyString(), ArgumentMatchers.any(RemotingCommand.class), ArgumentMatchers.anyLong(), ArgumentMatchers.any(InvokeCallback.class));
//        try {
//            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
//                    3 * 1000, CommunicationMode.ASYNC, new SendMessageContext(), defaultMQProducerImpl);
//            Fail.failBecauseExceptionWasNotThrown(InterruptedException.class);
//        } catch (InterruptedException e) {
//            Assertions.assertThat(e).hasMessage("Interrupted Exception in Test");
//        }
//    }
//
//    private SendMessageRequestHeader createSendMessageRequestHeader() {
//        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
//        requestHeader.setBornTimestamp(System.currentTimeMillis());
//        requestHeader.setTopic(topic);
//        requestHeader.setProducerGroup(group);
//        requestHeader.setQueueId(1);
//        requestHeader.setMaxReconsumeTimes(10);
//        return requestHeader;
//    }
}