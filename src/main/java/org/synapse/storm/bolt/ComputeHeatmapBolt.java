package org.synapse.storm.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.model.LatLng;
import org.synapse.helper.AddressConverter;
import org.synapse.helper.ConstantProperties;

import java.util.*;

/**
 * Created by developer on 10/4/15.
 */
public class ComputeHeatmapBolt extends BaseBasicBolt {

    Map<String, Integer> heatMap = new HashMap<String, Integer>();

    Map <Long,List<LatLng>> heatMaps;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        heatMaps = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple,BasicOutputCollector basicOutputCollector) {
         System.out.println("hello");
        try {
            if(isTickTuple(tuple)) {
                emitHeatMap(basicOutputCollector);
                return;
            } else {
                LatLng latLng = (LatLng) tuple.getValueByField("geocode");
                Long timeInterval = selectTimeInterval(tuple.getLongByField("time"));
                List<LatLng> checkins = getCheckinForInterval(timeInterval);
                //checkins.add(new AddressConverter(location).getLatLng());
                checkins.add(latLng);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time-interval","hotzones"));

    }

    private Long selectTimeInterval(Long time) {
        return time / (60 * 1000);
    }

    private boolean isTickTuple(Tuple tuple) {
        boolean isTickTupple = tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
        System.out.println(isTickTupple);
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private List<LatLng> getCheckinForInterval(Long interval) {
        List<LatLng> hotzones = heatMaps.get(interval);
        if(hotzones == null) {
            hotzones = new ArrayList<>();
            heatMaps.put(interval,hotzones);
        }
        return hotzones;
    }

    private void emitHeatMap(BasicOutputCollector outputCollector) {
        Long emitUptoInterval = selectTimeInterval(System.currentTimeMillis());
        Set<Long> intervalAvailable = heatMaps.keySet();

        for (Long interval : intervalAvailable) {
            if(interval <= emitUptoInterval) {
                List<LatLng> hotzones = heatMaps.get(interval);
                outputCollector.emit(new Values(interval,hotzones));
                // System.out.println("interval="+interval);
                //System.out.println("hotzones="+hotzones);
            }
        }
    }
   @Override
    public Map<String,Object> getComponentConfiguration(){
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,300);

        return conf;
    }

}
