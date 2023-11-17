package org.logstashplugins;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class RocketConsumer implements MessageListenerConcurrently {
    private Consumer<Map<String, Object>> consumer;

    public RocketConsumer(Consumer<Map<String, Object>> consumer) {
        this.consumer = consumer;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

        msgs.forEach(msg -> {
            try {
                System.out.println("receive msg id="+msg.getMsgId());
                consumer.accept(Collections.singletonMap("message", new String(msg.getBody(), StandardCharsets.UTF_8)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
