package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;


public class ConsumerFactory {
    public static DefaultMQPushConsumer getConsumer(Configuration configuration) throws MQClientException {
        RPCHook acl = new AclClientRPCHook(new SessionCredentials(configuration.get(ConfigSpecs.accessKey), configuration.get(ConfigSpecs.secretKey)));

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(acl);
        consumer.setConsumerGroup(configuration.get(ConfigSpecs.consumerGroup));
        consumer.setInstanceName(configuration.get(ConfigSpecs.instanceName));
        consumer.setNamesrvAddr(configuration.get(ConfigSpecs.namesrvAddr));
        consumer.subscribe(configuration.get(ConfigSpecs.topic), configuration.get(ConfigSpecs.tag));//topic可以细分，术语为tags; 可以专门订阅某种tag的topic
        consumer.setConsumeTimeout(configuration.get(ConfigSpecs.consumerTimeout));

        RocketMqTopicUtils.createTopic(configuration.get(ConfigSpecs.topic), configuration.get(ConfigSpecs.namesrvAddr), acl);
        return consumer;

    }
}
