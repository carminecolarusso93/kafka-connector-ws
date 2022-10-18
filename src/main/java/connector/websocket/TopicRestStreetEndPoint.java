package connector.websocket;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.websocket.EncodeException;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;


import connector.kafka.ConsumerWithStreetRest;

@ServerEndpoint(value = "/kafka-connector-rest-geojson/")
public class TopicRestStreetEndPoint {

	private HashMap<Session, List<String>> sessionTopicMap = new HashMap<>();
	private HashMap<Session, ConsumerWithStreetRest> sessionConsumerMap = new HashMap<>();

	@OnOpen
	public void onOpen(Session session) throws IOException, EncodeException {

		System.out.println("OnOpen : " + session.getId());

		try {

			ConsumerWithStreetRest consumer = new ConsumerWithStreetRest(session);
			sessionConsumerMap.put(session, consumer);

		} catch (Exception e) {
			e.printStackTrace();
			session.getBasicRemote().sendText("Error: " + e.getMessage());
		}

	}

	@OnMessage
	public void onMessage(Session session, String message) throws IOException, EncodeException {
		System.out.println("OnMessage : " + message);
		String[] topicArr = message.split(",");

		List<String> topicList = Arrays.asList(topicArr);
		if (sessionTopicMap.containsKey(session)) {
			List<String> oldTopics = sessionTopicMap.get(session);

			if (!(oldTopics.containsAll(topicList) && topicList.containsAll(oldTopics))) {
				this.sessionConsumerMap.get(session).shutdown();

				ConsumerWithStreetRest consumer = new ConsumerWithStreetRest(session);
				sessionConsumerMap.put(session, consumer);
				sessionTopicMap.put(session, topicList);
				consumer.startConsume(topicList);
			}

		} else {
			sessionTopicMap.put(session, topicList);
			this.sessionConsumerMap.get(session).startConsume(topicList);
		}

	}

	@OnClose
	public void onClose(Session session) throws IOException, EncodeException {
		System.out.println("OnClose !! ");

		this.sessionConsumerMap.get(session).shutdown();
		this.sessionConsumerMap.remove(session);
		this.sessionTopicMap.remove(session);

		session.close();

	}

	@OnError
	public void onError(Session session, Throwable throwable) throws IOException {
		System.out.println("OnError !! ");

		this.sessionConsumerMap.get(session).shutdown();
		this.sessionConsumerMap.remove(session);
		this.sessionTopicMap.remove(session);

		session.close();

		System.out.println(throwable.getCause());
		System.out.println(throwable.getMessage());
		throwable.printStackTrace();
	}

}
