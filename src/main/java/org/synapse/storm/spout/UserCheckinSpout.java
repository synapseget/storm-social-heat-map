package org.synapse.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.synapse.helper.ConstantProperties;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by developer on 9/4/15.
 */
public class UserCheckinSpout extends BaseRichSpout implements ConstantProperties {

    private SpoutOutputCollector spoutOutputCollector;

    private List<String> checkins = new ArrayList<String>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CHECKIN));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(System.getProperty(USER_HOME)+ "/development/workspace/"+ CHECKIN_FILE_PATH));
            try {
                String currentCheckin;
                while((currentCheckin = bufferedReader.readLine()) != null) {
                    checkins.add(currentCheckin);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        for(String checkin : checkins) {
            spoutOutputCollector.emit(new Values(checkin));
        }
    }
}
