package org.synapse.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.synapse.helper.ConstantProperties;

import java.util.Map;

/**
 * Created by developer on 10/4/15.
 */
public class AppendLocationBolt extends BaseRichBolt implements ConstantProperties {

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        // TODO Geo-logic with tuple from UserCheckinSpout
        System.out.println(tuple.getStringByField(CHECKIN)+" Location");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
