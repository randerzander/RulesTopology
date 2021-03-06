## Dynamic Rules Topology

Some streaming applications demand high availability, but require the ability to dynamically update or add to the rules applied to streams.

This topology allows end-users to supply python files to a directory in HDFS. The topology picks up these new/updated rules and passes them to a PyBolt which dynamically loads them as standard Python modules.

Many rule systems try to maintain state across multiple events leading to resiliency and scaling problems. Instead, this topology takes the approach of building and distributing windows across worker-nodes, and requires rules to handle an atomic package of all relevant events themselves.

For example, a keyed window is built from events in data/stream1.txt.

After the initial events have loaded, the window has:
```
abc: [123]
def: [456, 789]
```

After streaming events in data/stream2.txt, all rules in rulesDir will receive:
```
abc, new1, [123]
def, new2, [456, 789]
```

The diagram below depicts the topology. 'source1' is the window "filler". 'source2' is a second stream which needs to be enriched with all matching events from source1.

![Topology Diagram](/screenshots/topology.png?raw=true)

Anything returned from the rules is emitted as an output tuple. It's up to a downstream bolt to route output to its intended final destination. This lets rules focus on business logic instead of mechanics.

The rules work with Storm's standard Python tuple objects and need to implement the below Python interface:
```
def handle(tup):
  return tup.values[0].lower()
```

# Setup:

Clone and build the project:
```
git clone https://github.com/randerzander/RulesTopology
cd RulesTopology
cp /etc/hadoop/conf/core-site.xml multilang/
mvn clean package
```

Create HDFS directory:
```
su hdfs
hdfs dfs -mkdir /user/root
hdfs dfs -chown root /user/root
exit
hdfs dfs -put rulesDir /user/root/
```

Create Kafka topic:
```
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic source1
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic source2
```

Run the topology:
```
storm jar target/RulesTopology-SNAPSHOT.jar com.github.randerzander.RulesTopology topology.props
```

Populate the window:
```
/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list localhost:6667 --topic source1 < data/source1.txt
```

Inject new data in the stream
```
/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list localhost:6667 --topic source2 < data/source2.txt
```

**Notes**:

1. This topology uses a custom window implementation. Going forward, [Storm's out of the box windowing](http://storm.apache.org/releases/1.0.0/Windowing.html) should be used instead.

2. It uses a custom HDFSBolt to read rules from HDFS. For Storm .10+, use the [Apache HDFSSpout](https://github.com/apache/storm/tree/master/external/storm-hdfs) instead.

TODO:

1. Add auto-expiry for window tuples.

2. Add failover for windows via ZooKeeper leader elections.
