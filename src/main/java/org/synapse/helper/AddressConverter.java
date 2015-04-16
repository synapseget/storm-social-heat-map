package org.synapse.helper;

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.*;

import java.io.IOException;


/**
 * Created by sudhir on 15/4/15.
 */
public class AddressConverter {

    private LatLng latLng;

    public AddressConverter(String address) {
        this.setLatLng(computeLatAndLong(address));
    }


    public LatLng computeLatAndLong(String address) {

        GeocoderRequest request = new GeocoderRequestBuilder().setAddress(address).setLanguage("en").getGeocoderRequest();
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

        }
        return latLng;

    }

    public LatLng getLatLng() {
        return latLng;
    }

    public void setLatLng(LatLng latLng) {
        this.latLng = latLng;
    }
}
