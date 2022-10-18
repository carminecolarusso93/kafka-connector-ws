package database.neo4j.roadnetwork;

import model.Coordinate;
import model.Intersection;
import model.Street;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.neo4j.Neo4jDAOImpl;

import java.util.ArrayList;
import java.util.Set;


public class RoadNetworkLogic implements RoadNetworkLogicLocal{
    private static final Logger logger = LoggerFactory.getLogger(RoadNetworkLogic.class);
    String uri = "bolt://neo4j-promenade-lyon-neo4j-replica.promenade-lyon.svc.cluster.local:7687";
    String user = "neo4j";
    String password = "password";
    Neo4jDAOImpl database = new Neo4jDAOImpl(uri, user, password);

    public RoadNetworkLogic() {
    }

   

    @Override
    public ArrayList<Street> getStreetsFromArea(String areaname, int zoom, int decimateSkip) {

        ArrayList<Street> streets = database.getStreetsFromArea(areaname, zoom);
        if (decimateSkip > 0) {
            for (Street s : streets) {
                ArrayList<Coordinate> geometry = s.getGeometry();
                ArrayList<Coordinate> geometryFiltered = new ArrayList<>();
                int geometrySize = geometry.size();
                for (int i = 0; i < geometrySize; i = i + decimateSkip + 1) {
                    geometryFiltered.add(geometry.get(i));
                }
                if (geometrySize % (decimateSkip + 1)!= 0)
                    geometryFiltered.add(geometry.get(geometrySize-1));
                s.setGeometry(geometryFiltered);
                logger.info("Decimation of street: " + geometrySize + "/" + geometryFiltered.size());
            }
        }

        return streets;
    }

    @Override
    public Street getStreetFromId(long id){
        return database.getStreet(id);
    }
    
    @Override
    public ArrayList<Street> getStreetsFromIds(Set<Long> streetIdsSet){
        return database.getStreetsFromLinkIds(streetIdsSet);
    }

    
    @Override
    public ArrayList<Intersection> getTopCritical(int topK) {
        return database.getTopCriticalNodes(topK);
    }

    public void close(){
        database.closeConnection();
    }
}
