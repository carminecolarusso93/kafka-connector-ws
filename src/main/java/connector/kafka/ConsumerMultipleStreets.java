package connector.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.websocket.Session;

import com.google.gson.Gson;
import database.neo4j.roadnetwork.RoadNetworkLogic;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import model.Coordinate;
import model.KafkaMessage;
import model.Street;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;

import mil.nga.sf.geojson.Feature;
import mil.nga.sf.geojson.LineString;
import mil.nga.sf.geojson.Position;

public class ConsumerMultipleStreets extends Thread {

    private KafkaConsumer<String, String> consumer;

    // private static String bootstrapServer = "172.31.0.134:31090";
    private static String bootstrapServer = "kafka.promenade-lyon.svc.cluster.local:9092";
    private String keyDeserializer = StringDeserializer.class.getName();
    private String valueDeserializer = StringDeserializer.class.getName();

    private static String offsetReset = "earliest";
    private Session session;

    private AdminClientSingleton admin = AdminClientSingleton.getAdminClient();

    private List<String> topicList;

    // private final AtomicBoolean closed = new AtomicBoolean(false);

    private Gson g = new Gson();

    private RoadNetworkLogic rnl;

    public ConsumerMultipleStreets(Session session, RoadNetworkLogic rnl) {

        this.session = session;
        this.rnl = rnl;

        Properties properties = new Properties();

        try {
            // Configure the consumer
            properties = new Properties();
            // Point it to the brokers
            properties.setProperty("bootstrap.servers", bootstrapServer);
            // Set the consumer group (all consumers must belong to a group).
            properties.setProperty("group.id", session.getId());
            // Set how to serialize key/value pairs
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
            // When a group is first created, it has no offset stored to start reading from.
            // This tells it to start
            // with the earliest record in the stream.
            properties.setProperty("auto.offset.reset", offsetReset);
            consumer = new KafkaConsumer<>(properties);

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.out.println("ERRORE 1");
        }

    }

    public void startConsume(List<String> topics) {
        System.out.println("topics onmessage: " + topics.toString());
        ArrayList<String> topicsNorth = new ArrayList<String>();

        for (String s : topics) {
            topicsNorth.add(s + "-Northbound");
        }

        this.topicList = topicsNorth;
        try {
            start();
        } catch (OutOfMemoryError err) {
            System.err.println(err.getStackTrace());
            System.err.println("Errore creazione thread, chiusura consumer, eliminazione consumer group...");
            consumer.close();
            admin.closeConsumerGroup(session.getId());
        } catch (Exception e) {
            System.err.println(e.getStackTrace());
            System.err.println("ERRORE start" + e.getMessage());
            consumer.close();
            admin.closeConsumerGroup(session.getId());

        }

    }

    public void run() {

        try {
            // set consuming for while loop for read messages
            while (this.topicList == null) {
            }

            consumer.subscribe(topicList, new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("Assigned " + partitions);

                    for (TopicPartition tp : partitions) {

                        try {

                            Long newDate = System.currentTimeMillis() - (1000l * 60l * 3l + 200l);
                            Map<TopicPartition, Long> topLong = new HashMap<TopicPartition, Long>();
                            topLong.put(tp, newDate);
                            Map<TopicPartition, OffsetAndTimestamp> mapParTop = consumer.offsetsForTimes(topLong);

                            if (mapParTop.get(tp) != null) {
                                System.out.println("Offset find: " + mapParTop.get(tp).offset());
                                consumer.seek(tp, mapParTop.get(tp).offset());
                            } else {

                                Map<TopicPartition, Long> lastOffsetMap = consumer.endOffsets(partitions);
                                System.out.println("Seek to last offset: " + (lastOffsetMap.get(tp) - 1));
                                consumer.seek(tp, lastOffsetMap.get(tp) - 1);
                            }

                        } catch (Exception e) {
                            System.out.println("Exception: " + e.getMessage());
                        }
                    }
                }
            });

            while (!this.isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    if (records.count() != 0) {
                        ArrayList<String> geojsons = this.createGeoJsonMessage(records);
                        forward(geojsons);
                    }
            }

            consumer.close(Duration.ZERO);
            admin.closeConsumerGroup(session.getId());

        }
//		catch (InterruptedException e) {
//			if (!closed.get())
//				throw e;
//		}
        catch (OutOfMemoryError err) {
            err.printStackTrace();
            System.out.println(err.getMessage());
            System.out.println("ERRORE RUN: OutOfMemory");
        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.out.println("ERRORE RUN: Exception");
        }
        System.out.println("finally");
        
        try{
            consumer.close(Duration.ZERO);
            admin.closeConsumerGroup(session.getId());
        }catch(InterruptException e){
            System.err.println("#### InterruptException Consumer !!! ####");
            
        }
        

    }

    private ArrayList<String> createGeoJsonMessage(ConsumerRecords<String, String> records) {

        HashMap<Long, KafkaMessage> kafkaMessageMap = new HashMap<Long, KafkaMessage>();
        ArrayList<String> geojsons = new ArrayList<String>();

        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();
            KafkaMessage m = g.fromJson(message, KafkaMessage.class);
            kafkaMessageMap.put(m.getLinkid(), m);
            //System.out.println("Record: " + message);
        }

        Set<Long> ids = kafkaMessageMap.keySet();
        //for (Long id : ids) {
        //    System.out.println("-- DEBUG Strada: " + id);
        //}
        System.out.println("### ID STRADE RECUPERATI ###");
        // try {

        ArrayList<Street> streets = this.rnl.getStreetsFromIds(ids);

        for (Street street : streets) {
            ArrayList<Position> positions = new ArrayList<>();
            for (Coordinate c : street.getGeometry()) {
                Position p = new Position(c.getLongitude(), c.getLatitude());
                positions.add(p);
            }

            Map<String, Object> properties = new HashMap<>();
            properties.put("name", street.getName());
            properties.put("avgTravelTime", kafkaMessageMap.get(street.getLinkId()).getAvgTravelTime());
            properties.put("sdTravelTime", kafkaMessageMap.get(street.getLinkId()).getSdTravelTime());
            properties.put("numVehicles", kafkaMessageMap.get(street.getLinkId()).getNumVehicles());
            properties.put("aggPeriod", kafkaMessageMap.get(street.getLinkId()).getAggPeriod());
            properties.put("domainAggTimestamp", kafkaMessageMap.get(street.getLinkId()).getDomainAggTimestamp());
            properties.put("addTimestamp", kafkaMessageMap.get(street.getLinkId()).getAddTimestamp());
            properties.put("linkid", kafkaMessageMap.get(street.getLinkId()).getLinkid());
            properties.put("areaName", kafkaMessageMap.get(street.getLinkId()).getAreaName());

            Feature feature = new Feature(new LineString(positions));
            feature.setProperties(properties);

            String json = g.toJson(feature);

            geojsons.add(json);
        }

        return geojsons;

        // }
        // catch (Exception exception) {
        // System.err.println("ERRORE chiamata NEO4J !");
        // exception.printStackTrace();
        // System.err.println(exception.getMessage());
        //// closed.set(true);
        // this.interrupt();
        // return null;
        // }
    }

    private void forward(ArrayList<String> messages) throws IOException {
        try {
            for (String message : messages)
                session.getBasicRemote().sendText(message);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("ERRORE forward" + e.getMessage());
            session.close();
            this.interrupt();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        System.out.println("Shutdown consumer!");
        // closed.set(true);
        // consumer.wakeup();

        // chiusura consumer kafka e cancellazione consumer group
        // consumer.close(Duration.ZERO);
        // admin.closeConsumerGroup(session.getId());

        // interrupt sul thread
        this.interrupt();
    }
}
