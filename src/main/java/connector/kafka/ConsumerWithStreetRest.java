package connector.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.websocket.Session;
import com.google.gson.Gson;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import model.Coordinate;
import model.KafkaMessage;
import model.Street;

import org.apache.kafka.common.errors.WakeupException;

import mil.nga.sf.geojson.Feature;
import mil.nga.sf.geojson.LineString;
import mil.nga.sf.geojson.Position;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class ConsumerWithStreetRest extends Thread {

    private KafkaConsumer<String, String> consumer;

    private static String bootstrapServer = "kafka.promenade-lyon.svc.cluster.local:9092";
    private String keyDeserializer = StringDeserializer.class.getName();
    private String valueDeserializer = StringDeserializer.class.getName();
    private final String streetRestServiceUri = "http://promenade-frontend-promenade-lyon.apps.kube.rcost.unisannio.it/promenadeAreaNameService/rest/areaService/ids/streets?ids=";
    private static String offsetReset = "earliest";

    private Session session;

    private List<String> topicList;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private AdminClientSingleton admin = AdminClientSingleton.getAdminClient();

    private Gson g = new Gson();

    private CloseableHttpClient httpClient = HttpClients.createDefault();

    public ConsumerWithStreetRest(Session session) {

        this.session = session;

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
        start();

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

            while (!closed.get()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                System.out.println("-- DEBUG numero di record: " + records.count());
                if (records.count() != 0) {
                    ArrayList<String> geojsons = this.createGeoJsonMessage(records);
                    forward(geojsons);
                }

            }

        } catch (WakeupException e) {
            if (!closed.get())
                throw e;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.out.println("ERRORE RUN");
        } finally {
            consumer.close();
            admin.closeConsumerGroup(session.getId());

        }
    }

    private synchronized ArrayList<String> createGeoJsonMessage(ConsumerRecords<String, String> records)
            throws UnsupportedOperationException, IOException {

        HashMap<Long, KafkaMessage> kafkaMessageMap = new HashMap<Long, KafkaMessage>();
        ArrayList<String> geojsons = new ArrayList<String>();

        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();
            KafkaMessage m = g.fromJson(message, KafkaMessage.class);
            kafkaMessageMap.put(m.getLinkid(), m);
            System.out.println("Record: " + message);
        }

        System.out.println("-- DEBUG Ids strade: ");

        Set<Long> ids = kafkaMessageMap.keySet();
        for (Long id : ids) {
            System.out.println("-- DEBUG Strada: " + id);
        }

        ArrayList<Long> streetIds = new ArrayList<>(ids);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < streetIds.size() - 1; i++) {
            builder.append(streetIds.get(i) + ",");
        }
        builder.append((streetIds.get(streetIds.size() - 1)));

        System.out.println(builder);

        // Create an HttpGet object
        HttpGet httpget = new HttpGet(streetRestServiceUri + builder);

        // Execute the Get request
        CloseableHttpResponse httpresponse = httpClient.execute(httpget);

        String geojsonsString = null;
        Scanner sc = new Scanner(httpresponse.getEntity().getContent());
        while (sc.hasNext()) {
            geojsonsString = sc.nextLine();
            System.out.println("REST response: " + geojsonsString);
        }
        sc.close();
        httpresponse.close();

        if (geojsonsString != null) {

            Street[] streets = g.fromJson(geojsonsString, Street[].class);

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

                System.out.println("DEBUG FEATURE TO STRING JSON:   " + json);

                geojsons.add(json);
            }

            return geojsons;
        } else
            return geojsons;
    }

    private synchronized void forward(ArrayList<String> messages) throws IOException {
        try {

            for (String message : messages)
                session.getBasicRemote().sendText(message);

        } catch (IOException e) {
            e.printStackTrace();
            session.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();

        try {
            this.httpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}