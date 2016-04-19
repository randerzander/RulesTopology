package com.github.randerzander.StormCommon.bolts;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import static java.lang.Math.toIntExact;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.Config;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class HDFSBolt implements IRichBolt {
  private String folder;
  private FileSystem fs;
  private Path path;
  private int tickFrequency;
  private OutputCollector collector;

  public HDFSBolt(String folder, int tickFrequency){ this.folder = folder; this.tickFrequency = tickFrequency; }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
    this.collector = collector;
		Configuration conf = new Configuration();
    try{
      fs = FileSystem.get(conf);
      path = new Path(folder);
    } catch (Exception e){ e.printStackTrace(); System.exit(-1); }
  }

  @Override
  public void execute(Tuple tuple){
    try {
      Path path = new Path(folder);
      if (fs.getFileStatus(path).isDirectory()){
        FileStatus[] status = fs.listStatus(path);
        for (FileStatus x: status) {
          InputStream in = fs.open(x.getPath());
          byte[] bytes = new byte[toIntExact(x.getLen())];
          in.read(bytes);

          List<Object> output = new ArrayList<Object>();
          output.add(x.getPath().toString());
          output.add(new String(bytes, StandardCharsets.UTF_8));
          collector.emit(output);
        }
      }
    } catch (Exception e){ e.printStackTrace(); }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(new String[]{"path", "contents"}));
  }
  @Override
  public void cleanup(){}

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
