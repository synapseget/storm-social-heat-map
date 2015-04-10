package org.synapse.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.synapse.helper.ConstantProperties;
import org.synapse.storm.bolt.AppendLocationBolt;
import org.synapse.storm.spout.UserCheckinSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by developer on 10/4/15.
 */
public class CheckinTopologyRunner implements ConstantProperties {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_CHECKIN_SPOUT,new UserCheckinSpout());
        builder.setBolt(GEO_LOCATION_BOLT,new AppendLocationBolt()).shuffleGrouping(USER_CHECKIN_SPOUT);

        //StormTopology stormTopology = new StormTopology();
        LocalCluster localCluster = new LocalCluster();
        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_WORKERS, 4);
        conf.put(Config.TOPOLOGY_DEBUG, true);
        localCluster.submitTopology(HEAT_MAP_TOPOLOGY,conf,builder.createTopology());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        localCluster.killTopology(HEAT_MAP_TOPOLOGY);
        localCluster.shutdown();
    }

}
