/*
 *
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
 *
 */

package com.neuronbit.dbmq.spring.consumer;

import com.neuronbit.dbmq.client.consumer.DefaultMQPushConsumer;
import com.neuronbit.dbmq.client.producer.DefaultMQProducer;
import com.neuronbit.dbmq.exception.MQClientException;
import com.neuronbit.dbmq.spring.annotation.MessageConsumer;
import com.neuronbit.dbmq.spring.annotation.MessageTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.event.ContextRefreshedEvent;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

@Slf4j
public class MessageConsumerScanner implements ApplicationListener<ContextRefreshedEvent> {
    private final DefaultMQPushConsumer consumer;
    private final DefaultMQProducer producer;

    public MessageConsumerScanner(DefaultMQPushConsumer consumer, DefaultMQProducer producer) {
        this.consumer = consumer;
        this.producer = producer;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        scanMessageListener(event);

        start();
    }

    private void start() {
        try {
            consumer.start();
            producer.start();
        } catch (MQClientException e) {
            log.error("start consumer or producer failed", e);
            System.exit(1);
        }
    }

    private void scanMessageListener(ContextRefreshedEvent event) {
        ApplicationContext context = event.getApplicationContext();
        final String listenerPackage = context.getEnvironment().getProperty("dbmq.listener.package");
        if (isEmpty(listenerPackage)) {
            log.warn("dbmq.listener.package is not configured");
            return;
        } else {
            log.debug("dbmq.listener.package is set to '{}'", listenerPackage);
        }

        final Set<BeanDefinition> candidates = scanListener(listenerPackage);
        if (CollectionUtils.isEmpty(candidates)) {
            log.warn("there is no any listener find in the package '{}'", listenerPackage);
        }

        for (BeanDefinition candidate : candidates) {
            Class<?> clz;
            String beanClassName = candidate.getBeanClassName();

            try {
                clz = Class.forName(beanClassName);
            } catch (ClassNotFoundException e) {
                log.error("failed to subscriber listener: {}", beanClassName, e);
                continue;
            }

            final Method[] methods = clz.getMethods();
            for (Method method : methods) {
                final MessageTopic messageTopic = method.getAnnotation(MessageTopic.class);
                if (isNull(messageTopic)) {
                    continue;
                }
                final String topic = messageTopic.name();
                final Class<?> duplicateMessageDetectorClazz = messageTopic.duplicateMessageDetector();

                log.debug("found listener of topic '{}'", topic);

                Object listener = context.getBean(clz);
                if (isNull(listener)) {
                    log.debug("listener of service '{}' is not found in the spring context", topic);
                    continue;
                }

                try {
                    consumer.subscribe(topic, null);
                } catch (MQClientException e) {
                    log.error("subscribe topic {} failed", topic, e);
                    System.exit(1);
                    continue;
                }
                final MessageConsumerRegistry messageListener = (MessageConsumerRegistry) consumer.getMessageListener();
                messageListener.register(topic, new MessageConsumerAdaptor(listener, method));
            }
        }
    }

    private Set<BeanDefinition> scanListener(String listenerPackage) {
        final ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);

        scanner.addIncludeFilter((metadataReader, metadataReaderFactory) -> metadataReader
                .getAnnotationMetadata()
                .getAnnotationTypes()
                .contains(MessageConsumer.class.getCanonicalName()));

        final Set<BeanDefinition> candidateSet = new HashSet<>();
        final String[] targetPackages = listenerPackage.split(",");

        for (String targetPackage : targetPackages) {
            candidateSet.addAll(scanner.findCandidateComponents(targetPackage));
        }
        return candidateSet;
    }

}
