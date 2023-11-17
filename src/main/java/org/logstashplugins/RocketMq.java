package org.logstashplugins;

import co.elastic.logstash.api.*;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

@LogstashPlugin(name = "rocketmq")
public class RocketMq implements Input {
    private String id;
    private DefaultMQPushConsumer mqConsumer;
    private final CountDownLatch done = new CountDownLatch(1);

    public RocketMq(String id, Configuration config, Context context) {
        System.out.println("rocketmq construct");
        this.id = id;
        try {
            this.mqConsumer = ConsumerFactory.getConsumer(config);
        } catch (Exception e) {
            throw new RuntimeException("config initFail ", e);
        }
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        System.out.println("rocketmq start");

        try {
            mqConsumer.registerMessageListener(new RocketConsumer(consumer));
            mqConsumer.start();
            done.await();
        } catch (Exception e) {
            mqConsumer.shutdown();
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        System.out.println("rocketmq stop");

        done.countDown();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        System.out.println("rocketmq awaitStop");
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        System.out.println("rocketmq configSchema");
        return ConfigSpecs.getConfigSchema();
    }

    @Override
    public String getId() {
        System.out.println("rocketmq getId");
        return id;
    }
}
