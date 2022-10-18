package connector.websocket;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import javax.websocket.EncodeException;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;


import connector.kafka.Consumer;

import database.mongo.areanameservice.*;

@ServerEndpoint(value = "/kafka-connector-coordinates/")
public class CoordinatesMongoEndPoint {
	
	
	private HashMap<Session, List<String>> sessionTopicMap =  new HashMap<>();
	private HashMap<Session, Consumer> sessionConsumerMap =  new HashMap<>();
	
	private AreaNameLogicLocal anl = new AreaNameLogic();
	
	@OnOpen
    public void onOpen(Session session) throws IOException, EncodeException {
		
		
		System.out.println("OnOpen : "+session.getId());

        try {

			Consumer consumer = new Consumer(session);
			sessionConsumerMap.put(session, consumer);
			
		} catch (Exception e) {
			e.printStackTrace();
			session.getBasicRemote().sendText("Error: "+e.getMessage());
		}
        

    }
	
	
	@OnMessage
    public void onMessage(Session session,String message) throws IOException, EncodeException {
		      
		System.out.println("OnMessage: " + message);
		String[] cornerArr =  message.split(",");

		System.out.println("UpperLeftLon: " + Float.parseFloat(cornerArr[0]));
        System.out.println("UpperLeftLat: " + Float.parseFloat(cornerArr[1]));
        System.out.println("LowerRightLon: " + Float.parseFloat(cornerArr[2]));
        System.out.println("LowerRightLat: " + Float.parseFloat(cornerArr[3]));
        

		List<String> topicList = anl.getAreaNameFromCorners(Float.parseFloat(cornerArr[1]),Float.parseFloat(cornerArr[0]),Float.parseFloat(cornerArr[3]),Float.parseFloat(cornerArr[2]));


        if(sessionTopicMap.containsKey(session)){
			List<String> oldTopics = sessionTopicMap.get(session);
			if(!(oldTopics.containsAll(topicList) && topicList.containsAll(oldTopics))){
				this.sessionConsumerMap.get(session).shutdown();
				Consumer consumer = new Consumer(session);
				sessionConsumerMap.put(session, consumer);
				sessionTopicMap.put(session, topicList);
				consumer.startConsume(topicList);
			}
		}
		else{
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
		anl.close();
    }

    @OnError
    public void onError(Session session, Throwable throwable) throws IOException {
        // Do error handling here
    	System.out.println("OnError !! ");
    	this.sessionConsumerMap.get(session).shutdown();
    	
		this.sessionConsumerMap.remove(session);
		this.sessionTopicMap.remove(session);

    	session.close();
		anl.close();
    	
    	System.out.println(throwable.getCause());
    	System.out.println(throwable.getMessage());
    	throwable.printStackTrace();
    }
}
