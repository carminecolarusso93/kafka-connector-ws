package connector.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.websocket.Session;
import com.google.gson.Gson;
import database.neo4j.roadnetwork.*;
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

public class ConsumerWithStreet extends Thread {

	private KafkaConsumer<String, String> consumer;

	private static String bootstrapServer = "kafka.promenade-lyon.svc.cluster.local:9092";
	private String keyDeserializer = StringDeserializer.class.getName();
	private String valueDeserializer = StringDeserializer.class.getName();
	private static String offsetReset = "earliest";

	private Session session;

	private List<String> topicList;

	private final AtomicBoolean closed = new AtomicBoolean(false);
	private AdminClientSingleton admin = AdminClientSingleton.getAdminClient();

	private Gson g = new Gson();

	private RoadNetworkLogic rnl;

	public ConsumerWithStreet(Session session, RoadNetworkLogic rnl) {

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
			System.out.println("ERRORE CREAZIONE CONSUMER");
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
		}catch(Exception e){
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

			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(60000));
				System.out.println("-- DEBUG-withStreet numero di record: " + records.count());
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(record.value().toString());
					forward(record);
				}
			}
		} catch (WakeupException e) {
			if (!closed.get())
				throw e;
		} catch (OutOfMemoryError err) {
			err.printStackTrace();
			System.out.println(err.getMessage());
			System.out.println("ERRORE RUN: OutOfMemory");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("ERRORE RUN: Exception");
		} finally {
			consumer.close();
			admin.closeConsumerGroup(session.getId());
		}
	}

	private synchronized void forward(ConsumerRecord<String, String> record) {
		try {
			String message = record.value();
			KafkaMessage m = g.fromJson(message, KafkaMessage.class);

			System.out.println("DEBUG KAFKA-MESSAGE AFTER JSON PARSING:");
			System.out.println(m.toString());

			// Creazione GeoJson
			// Recupero strada
			Long linkId = m.getLinkid();

			Street street = this.rnl.getStreetFromId(linkId);
			System.out.println("DEBUG RECUPERO STRADA: " + street.toString());

			ArrayList<Position> positions = new ArrayList<>();
			for (Coordinate c : street.getGeometry()) {
				Position p = new Position(c.getLongitude(), c.getLatitude());
				positions.add(p);
			}

			Map<String, Object> properties = new HashMap<>();
			properties.put("name", street.getName());
			properties.put("avgTravelTime", m.getAvgTravelTime());
			properties.put("sdTravelTime", m.getSdTravelTime());
			properties.put("numVehicles", m.getNumVehicles());
			properties.put("aggPeriod", m.getAggPeriod());
			properties.put("domainAggTimestamp", m.getDomainAggTimestamp());
			properties.put("addTimestamp", m.getAddTimestamp());
			properties.put("linkid", m.getLinkid());
			properties.put("areaName", m.getAreaName());

			Feature feature = new Feature(new LineString(positions));
			feature.setProperties(properties);

			String json = g.toJson(feature);

			System.out.println("DEBUG FEATURE TO STRING JSON:   " + json);

			session.getBasicRemote().sendText(json);

		} catch (Exception e) {
			System.err.println("ERRORE BROADCAST! " + session.getId());
			e.printStackTrace();
			consumer.close();
			admin.closeConsumerGroup(session.getId());

		}
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
}
