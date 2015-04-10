package org.synapse.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.synapse.storm.bolt.AppendLocationBolt;
import org.synapse.storm.spout.UserCheckinSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by developer on 10/4/15.
 */
public class CheckinTopologyRunner {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("user-chckin-input",new UserCheckinSpout());
        builder.setBolt("checkin-append-location",new AppendLocationBolt()).shuffleGrouping("user-chckin-input");

        //StormTopology stormTopology = new StormTopology();
        LocalCluster localCluster = new LocalCluster();
        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_WORKERS, 4);
        conf.put(Config.TOPOLOGY_DEBUG, true);
        localCluster.submitTopology("user-checkin-topology",conf,builder.createTopology());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        localCluster.killTopology("user-checkin");
        localCluster.shutdown();
    }

}
