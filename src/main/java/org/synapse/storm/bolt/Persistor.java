package org.synapse.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.google.code.geocoder.model.LatLng;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by developer on 22/4/15.
 */
public class Persistor extends BaseBasicBolt {
    private final Logger logger = LoggerFactory.getLogger(Persistor.class);
   // private Jedis jedis;
   // private ObjectMappe objectMapper;
   Map <Long,List<LatLng>> heatMaps;
    @Override
    public void prepare(Map stormConf,
                        TopologyContext context) {
        heatMaps = new HashMap<>();
    }
    @Override
    public void execute(Tuple tuple,
                        BasicOutputCollector outputCollector) {
        Long timeInterval = tuple.getLongByField("time-interval");
        List<LatLng> hz = (List<LatLng>) tuple.getValueByField("hotzones");
        List<String> hotzones = asListOfStrings(hz);
        try {
            String key = "checkins-" + timeInterval;
             System.out.println("interval="+timeInterval);
             System.out.println("hotzones="+hotzones);
        } catch (Exception e) {
            logger.error("Error persisting for time: " + timeInterval, e);
        }

    }
    private List<String> asListOfStrings(List<LatLng> hotzones) {
        List<String> hotzonesStandard = new ArrayList<>(hotzones.size());
        for (LatLng geoCoordinate : hotzones) {
            hotzonesStandard.add(geoCoordinate.toUrlValue());
        }
        return hotzonesStandard;
    }
    @Override
    public void cleanup() {
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields to be declared #H
    }

}
