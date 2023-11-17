package org.logstashplugins;

import co.elastic.logstash.api.PluginConfigSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ConfigSpecs {
    public static final PluginConfigSpec<String> namesrvAddr = PluginConfigSpec.stringSetting("namesrv_addr");

    public static final PluginConfigSpec<String> consumerGroup = PluginConfigSpec.stringSetting("consumer_group");

    public static final PluginConfigSpec<String> topic = PluginConfigSpec.stringSetting("topic");

    public static final PluginConfigSpec<String> tag = PluginConfigSpec.stringSetting("tag","*");

    public static final PluginConfigSpec<String> accessKey = PluginConfigSpec.stringSetting("access_key");

    public static final PluginConfigSpec<String> secretKey = PluginConfigSpec.stringSetting("secret_key");

    public static final PluginConfigSpec<String> instanceName = PluginConfigSpec.stringSetting("instance_name", "logstash"+new Random().nextInt(100));

    public static final PluginConfigSpec<Long> consumerTimeout = PluginConfigSpec.numSetting("consumer_timeout",15l);

    public static final PluginConfigSpec<String> endpoint = PluginConfigSpec.stringSetting("endpoint","");

    public static List<PluginConfigSpec<?>> getConfigSchema() {
        return Arrays.asList(namesrvAddr, consumerGroup, topic, tag, accessKey, secretKey, instanceName, consumerTimeout, endpoint);
    }

}
