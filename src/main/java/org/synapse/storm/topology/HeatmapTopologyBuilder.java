package org.synapse.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.synapse.helper.ConstantProperties;
import org.synapse.storm.bolt.ComputeHeatmapBolt;
import org.synapse.storm.bolt.GeocodeLookupBolt;
import org.synapse.storm.bolt.Persistor;
import org.synapse.storm.spout.UserCheckinSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by developer on 10/4/15.
 */
public class HeatmapTopologyBuilder {

    public StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(ConstantProperties.USER_CHECKIN_SPOUT,new UserCheckinSpout());
        builder.setBolt(ConstantProperties.GEO_LOCATION_BOLT,new GeocodeLookupBolt()).shuffleGrouping(ConstantProperties.USER_CHECKIN_SPOUT);
        builder.setBolt("heatmap-bolt",new ComputeHeatmapBolt()).globalGrouping(ConstantProperties.GEO_LOCATION_BOLT);
        builder.setBolt("persistor",new Persistor()).shuffleGrouping("heatmap-bolt");
        return builder.createTopology();
        /*LocalCluster localCluster = new LocalCluster();
        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,60);
        localCluster.submitTopology(HEAT_MAP_TOPOLOGY,conf,builder.createTopology());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        localCluster.killTopology(HEAT_MAP_TOPOLOGY);
        localCluster.shutdown();*/
    }

}
