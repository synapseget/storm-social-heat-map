package org.synapse.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import org.synapse.helper.ConstantProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by developer on 21/4/15.
 */
public class HeatMapTopologyRunner{

    public static void main(String[] args) {
        Config config = new Config();
        StormTopology topology = new HeatmapTopologyBuilder().build();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(ConstantProperties.HEAT_MAP_TOPOLOGY,config,topology);
    }
}
