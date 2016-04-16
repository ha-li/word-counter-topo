package com.gecko.topo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gecko.topo.bolts.WordCounter;
import com.gecko.topo.bolts.WordNormalizer;
import com.gecko.topo.spouts.FileSpoutReader;
//import com.gecko.topo.spouts.WordReader;

/**
 * Created by hlieu on 4/4/16.
 */
public class WordCountTopology {

    private static final Integer BOLT_PARALLEL = 2;

    private static final Integer SPOUT_PARALLEL = 1;

    public static void main(String[] args) throws InterruptedException {

        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        // file-reader
        builder.setSpout("word-reader", new FileSpoutReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), BOLT_PARALLEL).fieldsGrouping("word-normalizer", new Fields("word")) ;

        // create the config, set the words file
        Config config = new Config();
        config.put("wordsfile", args[0]);
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, SPOUT_PARALLEL);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Word-Count-Topology", config, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
