package org.synapse.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by developer on 9/4/15.
 */
public class UserCheckinSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private List<String> checkins = new ArrayList<String>();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("checkin"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        checkins = Arrays.asList("checkin1","checkin3","checkin2");
    }

    @Override
    public void nextTuple() {
        for(String checkin : checkins) {
            spoutOutputCollector.emit(new Values(checkin));
        }
    }
}
