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

import connector.kafka.AdminClientSingleton;
import connector.kafka.ConsumerMultipleStreets;
import database.neo4j.roadnetwork.RoadNetworkLogic;

@ServerEndpoint(value = "/kafka-connector-geojson-multiple/")
public class TopicMultipleStreetsEndPoint {

	private HashMap<Session, List<String>> sessionTopicMap = new HashMap<>();
	private HashMap<Session, ConsumerMultipleStreets> sessionConsumerMap = new HashMap<>();

	public RoadNetworkLogic rnl = new RoadNetworkLogic();

	@OnOpen
	public void onOpen(Session session) throws IOException, EncodeException {

		System.out.println("OnOpen : " + session.getId());

		try {

			ConsumerMultipleStreets consumer = new ConsumerMultipleStreets(session, rnl);
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

				ConsumerMultipleStreets consumer = new ConsumerMultipleStreets(session, rnl);
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
	public void onClose(Session session) throws IOException {
		System.out.println("OnClose !! ");

		this.sessionConsumerMap.get(session).shutdown();
		
		try {
			
			//Attendiamo chiusura thread prima di fermare driver neo4j
			System.out.println(" pre join ");
			if(!this.sessionConsumerMap.get(session).isInterrupted()){
//				Thread.sleep(10);
				this.sessionConsumerMap.get(session).join();
			}
			System.out.println(" post join ");
			rnl.close();

			//Chiusura consumer group
			AdminClientSingleton.getAdminClient().closeConsumerGroup(session.getId());

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("ERRORE close" + e.getMessage());
		}

		this.sessionConsumerMap.remove(session);
		this.sessionTopicMap.remove(session);

		session.close();

	}

	@OnError
	public void onError(Session session, Throwable throwable) throws IOException {
		System.out.println("OnError !! ");

//		this.sessionConsumerMap.get(session).shutdown();
//		rnl.close();
//
//		this.sessionConsumerMap.remove(session);
//		this.sessionTopicMap.remove(session);
//
//		System.out.println(throwable.getCause());
//		System.out.println(throwable.getMessage());
//		throwable.printStackTrace();
//		session.close();
		this.onClose(session);
	}

}
