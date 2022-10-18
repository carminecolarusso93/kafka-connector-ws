package database.mongo.areanameservice;

import java.util.ArrayList;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;

import database.mongo.MongoConnectionManager;
import model.Area;

public class AreaNameLogic implements AreaNameLogicLocal {
    MongoConnectionManager connectionManager = new MongoConnectionManager();

    @Override
    public ArrayList<String> getAreaNameFromCorners(float upperLeftLon, float upperLeftLat, float lowerRightLon,
            float lowerRightLat) {

        ArrayList<Position> positions = new ArrayList<Position>();
        positions.add(new Position(upperLeftLon, upperLeftLat));
        positions.add(new Position(lowerRightLon, upperLeftLat));
        positions.add(new Position(lowerRightLon, lowerRightLat));
        positions.add(new Position(upperLeftLon, lowerRightLat));
        positions.add(new Position(upperLeftLon, upperLeftLat));

        Polygon polygon = new Polygon(positions);

        FindIterable<Area> result = connectionManager.getCollection(null, Area.class)
                .find(Filters.geoIntersects("polygon", polygon));
        System.out.println("Risultato getCollection: " + result.toString());
        ArrayList<String> areaNames = new ArrayList<String>();

        for (Area a : result) {
            areaNames.add(a.getNom_com() + "-Northbound");
            System.out.println("DEBUG: area aggiunta: " + a.getNom_com() + "-Northbound");
        }

        return areaNames;
    }

    @Override
    public void close() {
        connectionManager.close();
    }
}