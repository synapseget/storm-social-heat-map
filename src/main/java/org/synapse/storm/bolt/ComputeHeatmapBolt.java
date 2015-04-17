package org.synapse.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.code.geocoder.model.LatLng;
import org.synapse.helper.AddressConverter;
import org.synapse.helper.ConstantProperties;

import java.util.*;

/**
 * Created by developer on 10/4/15.
 */
public class ComputeHeatmapBolt extends BaseRichBolt implements ConstantProperties {

    Map<String, Integer> heatMap = new HashMap<String, Integer>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {

        try {
            String location = tuple.getStringByField(CHECKIN);
            if (heatMap.containsKey(location)) {
                heatMap.put(location, heatMap.get(location) + 1);
            } else {
                heatMap.put(location, 1);
            }
            Map sortedHeatMap = sortByValue(heatMap);
            System.out.println(sortedHeatMap);
            List hotZoneAsLatLong = new ArrayList();
            for (Object zone : sortedHeatMap.keySet()) {
                LatLng zoneLatLng = new AddressConverter((String) zone).getLatLng();
                if(zoneLatLng != null) {
                    hotZoneAsLatLong.add(zoneLatLng.getLat() +","+ zoneLatLng.getLng() );
                }
            }
            System.out.println(hotZoneAsLatLong);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map sortByValue(Map map) {
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator<Object>() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o1)).getValue())
                        .compareTo(((Map.Entry) (o2)).getValue());
            }
        });

        Map result = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

}
