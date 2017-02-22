package com.hadoop.test;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;


public class KafkaSpoutTest {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTest.class);

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            LOG.info(tuple.toString());
        }

    }

    public static void main(String[] args) throws Exception {
        if( args.length<2) {
            System.out.println("Require parameter: <Zookeeper Connection> <topic-name> [security-protocol] [keytab-file] [kerberos-principal]");
            System.exit(1);
        }

        String zkConnString=args[0];
        String topicName=args[1];
        String securityProtocol="PLAINTEXTSASL";
        String keytabFile="/etc/security/keytabs/storm.headless.keytab";
        String principal="storm";

        if (args.length>3) {
            securityProtocol = args[2];
            keytabFile=args[3];
            principal=args[4];
        }

        String topologyName="KafkaSpoutTest";

        BrokerHosts zkHosts= new ZkHosts(zkConnString);
        SpoutConfig kafkaSpoutConfig = new SpoutConfig(zkHosts,topicName,"","KafkaSpoutTest");
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpoutConfig.securityProtocol=securityProtocol;
        kafkaSpoutConfig.startOffsetTime=kafka.api.OffsetRequest.EarliestTime();

        LOG.info("+++ zkRoot: " + kafkaSpoutConfig.zkRoot);
        LOG.info("+++ zkServers: " + kafkaSpoutConfig.zkServers);
        LOG.info("+++ zkPort: " + kafkaSpoutConfig.zkPort);




        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words",new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("words");

        Config config = new Config();
        config.put("storm.keytab.file", keytabFile);
        config.put("storm.kerberos.principal",principal);
        config.setNumWorkers(1);
        config.setMaxTaskParallelism(1);

        StormSubmitter.submitTopology(topologyName,config,builder.createTopology());

    }
}
