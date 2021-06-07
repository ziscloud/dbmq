package com.neuronbit.lrdatf.client.consumer;

import com.neuronbit.lrdatf.client.impl.factory.MQClientInstance;
import com.neuronbit.lrdatf.common.constant.LoggerName;
import com.neuronbit.lrdatf.remoting.common.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceService extends ServiceThread {
    private static long waitInterval =
            Long.parseLong(System.getProperty(
                    "rocketmq.client.rebalance.waitInterval", "20000"));
    private final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            this.waitForRunning(waitInterval);
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
