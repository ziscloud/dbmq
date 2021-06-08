package com.neuronbit.lrdatf.spring.enums;

import com.neuronbit.lrdatf.client.consumer.AllocateMessageQueueStrategy;
import com.neuronbit.lrdatf.client.consumer.rebalance.AllocateMessageQueueAveragely;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum AllocateMessageQueueMode {
    AVG(AllocateMessageQueueAveragely.class);

    private final Class<? extends AllocateMessageQueueStrategy> clazz;

    AllocateMessageQueueMode(Class<? extends AllocateMessageQueueStrategy> clazz) {
        this.clazz = clazz;
    }

    public AllocateMessageQueueStrategy getInstance() {
        try {
            return this.clazz.newInstance();
        } catch (Exception e) {
            log.error("can not create instance of {}", clazz.getName(), e);
            return null;
        }
    }
}
