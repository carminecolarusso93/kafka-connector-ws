package connector.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.websocket.Session;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.errors.WakeupException;

public class Consumer extends Thread {

	private KafkaConsumer<String, String> consumer;

	private static String bootstrapServer = "kafka.promenade-lyon.svc.cluster.local:9092";
	private String keyDeserializer = StringDeserializer.class.getName();
	private String valueDeserializer = StringDeserializer.class.getName();

	private static String offsetReset = "earliest";
	private Session session;

	private List<String> topicList;

	private final AtomicBoolean closed = new AtomicBoolean(false);
	private AdminClientSingleton admin = AdminClientSingleton.getAdminClient();

	public Consumer(Session session) {

		this.session = session;

		Properties properties;

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
		this.topicList = topics;
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
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(record.value().toString());
					forward(record);
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

	private synchronized void forward(ConsumerRecord<String, String> record) throws IOException {
		try {
			session.getBasicRemote().sendText(record.value());
		} catch (IOException e) {
			e.printStackTrace();
			session.close();
		}
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
}
