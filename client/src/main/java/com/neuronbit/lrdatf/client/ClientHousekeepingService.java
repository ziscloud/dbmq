package com.neuronbit.lrdatf.client;

import com.neuronbit.lrdatf.client.comsumer.MQClientInstance;
import com.neuronbit.lrdatf.common.constant.LoggerName;
import com.neuronbit.lrdatf.remoting.common.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientHousekeepingService extends ServiceThread {
    private static long waitInterval =
            Long.parseLong(System.getProperty(
                    "rocketmq.client.rebalance.waitInterval", "20000"));
    private final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);
    private final MQClientInstance mqClientFactory;

    public ClientHousekeepingService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public String getServiceName() {
        return ClientHousekeepingService.class.getSimpleName();
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            this.waitForRunning(waitInterval);
            this.mqClientFactory.scanException();
        }

        log.info(this.getServiceName() + " service end");
    }
}
