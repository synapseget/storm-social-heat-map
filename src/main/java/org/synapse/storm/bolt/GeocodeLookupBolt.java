package org.synapse.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.*;

import java.io.IOException;
import java.util.Map;

/**
 * Created by developer on 22/4/15.
 */
public class GeocodeLookupBolt extends BaseBasicBolt {
    private Geocoder geocoder;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        geocoder = new Geocoder();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Long time = tuple.getLongByField("time");
        GeocoderRequest request = new GeocoderRequestBuilder().setAddress(tuple.getStringByField("checkin")).setLanguage("en").getGeocoderRequest();
        GeocodeResponse response = null;
        LatLng latLng = null;
        try {
            response = new Geocoder().geocode(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        GeocoderStatus status = response.getStatus();
        if (GeocoderStatus.OK.equals(status)) {
            GeocoderResult firstResult = response.getResults().get(0);
            latLng = firstResult.getGeometry().getLocation();
            basicOutputCollector.emit(new Values(time,latLng));

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time","geocode"));
    }
}
