package database.mongo.areanameservice;

import java.util.ArrayList;

public interface AreaNameLogicLocal {

    public ArrayList<String> getAreaNameFromCorners(float upperLeftLon, float upperLeftLat, float lowerRightLon,
            float lowerRightLat);

    public void close();

}