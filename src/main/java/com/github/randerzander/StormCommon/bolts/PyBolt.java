package com.github.randerzander.StormCommon.bolts;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.Config;

import java.util.ArrayList;
import java.util.Map;

public class PyBolt extends ShellBolt implements IRichBolt {
    private ArrayList<String> outputFields;
    private int tickFrequency = -1;

    public PyBolt(String filename, String[] fields, int tickFrequency) {
      super("python", filename);
      outputFields = new ArrayList<String>();
      for(String field: fields) outputFields.add(field);
      this.tickFrequency = tickFrequency;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields(outputFields)); }

    @Override
    public Map<String, Object> getComponentConfiguration(){
      if (this.tickFrequency != -1){
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequency);
        return conf;
      }
      return null;
    }
}
