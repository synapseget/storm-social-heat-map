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
import java.util.*;

/**
 * Created by developer on 9/4/15.
 */
public class UserCheckinSpout extends BaseRichSpout{

    private SpoutOutputCollector spoutOutputCollector;

    private Map<Long,String> checkins = new HashMap<>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time",ConstantProperties.CHECKIN));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(System.getProperty(ConstantProperties.USER_HOME)+ "/development/workspace/"+ ConstantProperties.CHECKIN_FILE_PATH));
            try {
                String currentCheckin;
                while((currentCheckin = bufferedReader.readLine()) != null) {
                    String[] checkin = currentCheckin.split(",",2);
                    checkins.put((Long.parseLong(checkin[0])),checkin[1]);
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
        for (Map.Entry<Long, String> entry : checkins.entrySet()) {
            spoutOutputCollector.emit(new Values(entry.getKey(),entry.getValue()));
        }
    }
}
