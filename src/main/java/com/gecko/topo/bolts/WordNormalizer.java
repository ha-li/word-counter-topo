package com.gecko.topo.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by hlieu on 04/3/16.
 */
public class WordNormalizer extends BaseBasicBolt {
    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getStringByField("line");
        String[] words = sentence.split(" ");
        for(String word : words) {
            word = word.trim();
            if(!word.isEmpty()) {
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }
        }
        //collector.ack(input);
    }



    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("word")); }
}
