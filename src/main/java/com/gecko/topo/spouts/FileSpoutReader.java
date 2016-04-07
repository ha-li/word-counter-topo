package com.gecko.topo.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hlieu on 04/3/16.
 */
public class FileSpoutReader extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;

    public void ack(Object msgId) {System.out.println("OK " + msgId);}
    public void close() {}
    public void fail(Object msgId) {System.out.println("Failed " + msgId);}

    public void nextTuple() {
        if(completed) {
            try {
                Thread.sleep(1000);
            }catch(InterruptedException e) {

            }
            return;
        }

        String str;
        BufferedReader reader = new BufferedReader(this.fileReader);
        try {
            StringBuffer sb = new StringBuffer();
            while( (str = reader.readLine()) != null) {
                //sb.append(str + "\n");
                this.collector.emit(new Values(str), str);
            }

            //this.collector.emit(new Values(sb.toString()), "all-lines");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            completed = true;
        }
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("wordsfile").toString());
        } catch (FileNotFoundException fe) {
            throw new RuntimeException(fe);
        }
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("line"));}
}
