package org.logstashplugins;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.topic.UpdateTopicSubCommand;

import java.util.LinkedList;
import java.util.List;

public class RocketMqTopicUtils {

    public static void createTopic(String topic, String nameServer, RPCHook rpcHook) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, nameServer);
        getClusterInfo(nameServer).getClusterAddrTable().keySet().forEach((clusterName) -> {
            createTopic(null, clusterName, topic, rpcHook);
        });
    }


    private static ClusterInfo getClusterInfo(String namesrvAddr) {
        if (StringUtils.isBlank(namesrvAddr)) {
            return new ClusterInfo();
        }
        ClusterInfo clusterInfo = null;
        try {
            DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt(5000L);
            mqAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
            mqAdminExt.setNamesrvAddr(namesrvAddr);
            mqAdminExt.start();
            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            mqAdminExt.shutdown();
        } catch (InterruptedException | RemotingConnectException | RemotingSendRequestException
                 | MQBrokerException | MQClientException | RemotingTimeoutException e) {
            throw new RuntimeException("error when RocketMQTopicUtil getClusterInfo,namesrvAddr:" + namesrvAddr, e);
        }
        return clusterInfo;
    }

    private static boolean createTopic(String brokerAddr, String clusterName, String topic, RPCHook rpcHook) {
        if (StringUtils.isBlank(topic)) {
            return false;
        }
        List<String> argList = new LinkedList<>();
        argList.add("-t " + topic);
        if (StringUtils.isNotBlank(brokerAddr)) {
            argList.add("-b " + brokerAddr.trim());
        } else {
            argList.add("-c " + clusterName.trim());
        }
        return createTopic(argList.toArray(new String[0]), rpcHook);
    }


    private static boolean createTopic(String[] subargs, RPCHook rpcHook) {
//        String[] subargs = new String[]{
//                "-b 10.1.4.231:10911brokerList",//给指定broker创建topic
//                "-t topicName",
//                "-c clusterName",//-b和-c二选一，给整个集群创建Topic
//                "-r 8",
//                "-w 8",
//                "-p 6",
//                "-o false",
//                "-u false",
//                "-s false"};
        try {
            UpdateTopicSubCommand cmd = new UpdateTopicSubCommand();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            final Options updateTopicOptions = cmd.buildCommandlineOptions(options);
            final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(),
                    subargs, updateTopicOptions, new PosixParser());
            cmd.execute(commandLine, updateTopicOptions, rpcHook);
        } catch (SubCommandException e) {
            throw new RuntimeException("error when RocketMQTopicUtil UpdateTopicSubCommand.execute", e);
        }
        return true;
    }
}
