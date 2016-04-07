package com.gecko.topo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gecko.topo.bolts.WordCounter;
import com.gecko.topo.bolts.WordNormalizer;
import com.gecko.topo.spouts.WordReader;

/**
 * Created by hlieu on 4/4/16.
 */
public class WordCountTopology {

    private static final Integer BOLT_PARALLEL_WORD_COUNT = 1;

    private static final Integer SPOUT_MAX_COUNT = 1;

    public static void main(String[] args) throws InterruptedException {

        // create the topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), BOLT_PARALLEL_WORD_COUNT).fieldsGrouping("word-normalizer", new Fields("word"));

        // create the config, set the words file
        Config config = new Config();
        config.put("wordsfile", args[0]);
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, SPOUT_MAX_COUNT);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Word-Count-Topology", config, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
