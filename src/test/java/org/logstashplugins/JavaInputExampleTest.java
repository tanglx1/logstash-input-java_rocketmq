package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class JavaInputExampleTest {

    @Test
    public void testJavaInputExample() {
        String prefix = "This is message";
        long eventCount = 5;
        Map<String, Object> configValues = new HashMap<>();
        configValues.put(ConfigSpecs.namesrvAddr.name(), "localhost:9876");
        configValues.put(ConfigSpecs.topic.name(), "topicName1");
        configValues.put(ConfigSpecs.consumerGroup.name(), "logstash");
        configValues.put(ConfigSpecs.accessKey.name(), "rocketmq2");
        configValues.put(ConfigSpecs.secretKey.name(), "12345678");
        Configuration config = new ConfigurationImpl(configValues);
        RocketMq input = new RocketMq("test-id", config, null);
        TestConsumer testConsumer = new TestConsumer();
        CompletableFuture.runAsync(()->{
            input.start(testConsumer);
        });
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<Map<String, Object>> events = testConsumer.getEvents();
        Assert.assertTrue(events.size()>0);
    }

    private static class TestConsumer implements Consumer<Map<String, Object>> {

        private List<Map<String, Object>> events = new ArrayList<>();

        @Override
        public void accept(Map<String, Object> event) {
            synchronized (this) {
                events.add(event);
            }
        }

        public List<Map<String, Object>> getEvents() {
            return events;
        }
    }

}
