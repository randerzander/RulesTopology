package com.github.randerzander;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.SpoutDeclarer;

import com.github.randerzander.StormCommon.Utils;
import com.github.randerzander.StormCommon.bolts.PyBolt;
import com.github.randerzander.StormCommon.bolts.HDFSBolt;

import java.util.HashMap;

public class RulesTopology {
    public static void main(String[] args) throws Exception {
      HashMap<String, String> props = Utils.getPropertiesMap(args[0]);
      TopologyBuilder builder = new TopologyBuilder();
      Config conf = new Config();

      //Setup spout sources
      builder.setSpout("source1-spout", new KafkaSpout(setupSpout(props, "source1")));
      builder.setSpout("source2-spout", new KafkaSpout(setupSpout(props, "source2")));

      conf.put("default_delimiter", props.get("parser.default_delimiter"));
      builder.setBolt("source1-parser", new PyBolt("parser.py", new String[]{"key", "payload"}, -1))
        .shuffleGrouping("source1-spout");
      builder.setBolt("source2-parser", new PyBolt("parser.py", new String[]{"key", "payload"}, -1))
        .shuffleGrouping("source2-spout");

      //Windows built here
      conf.put("filler_component", props.get("window.filler_component"));
      builder.setBolt("window", new PyBolt("window.py", new String[]{"event", "intersects"}, -1))
        .shuffleGrouping("source1-parser")
        .shuffleGrouping("source2-parser");

      //Periodically emits definitions of rules from HDFS to rule-executors
      builder.setBolt("rule-definitions", new HDFSBolt(props.get("hdfs.rulesDir"), Integer.parseInt(props.get("hdfs.rulesRefreshFrequency"))));

      //Rules execute against enriched events emanating from windows
      builder.setBolt("rule-execution", new PyBolt("rules.py", new String[]{"intersects", "event"}, -1))
        .allGrouping("rule-definitions")
        .shuffleGrouping("window");

      conf.setNumWorkers(Integer.parseInt(props.get("storm.numWorkers")));
      Utils.run(builder, props.get("storm.topologyName"), conf, props.get("storm.killIfRunning").equals("true"), props.get("storm.localMode").equals("true"));
    }

    public static SpoutConfig setupSpout(HashMap<String, String> props, String spoutName){
      SpoutConfig spoutConfig = new SpoutConfig(
        new ZkHosts(props.get("zk.hosts")),
        props.get(spoutName+".topic"), props.get("zk.rootNode"), props.get(spoutName+".consumerGroupId")
      );
      spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
      return spoutConfig;
    }
}
