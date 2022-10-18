package database.neo4j.roadnetwork;

import model.Intersection;
import model.Street;
import java.util.ArrayList;
import java.util.Set;


public interface RoadNetworkLogicLocal {
    public ArrayList<Street> getStreetsFromArea(String areaname, int zoom, int decimateSkip);

    public Street getStreetFromId(long id);

    public ArrayList<Intersection> getTopCritical(int topK);

    public ArrayList<Street> getStreetsFromIds(Set<Long> streetIdsSet);

}