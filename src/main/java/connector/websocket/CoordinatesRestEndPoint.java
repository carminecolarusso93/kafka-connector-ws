package connector.websocket;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import javax.websocket.EncodeException;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;



import connector.kafka.Consumer;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


@ServerEndpoint(value = "/kafka-connector-rest-coordinates/")
public class CoordinatesRestEndPoint {
	
	
	private HashMap<Session, List<String>> sessionTopicMap =  new HashMap<>();
	private HashMap<Session, Consumer> sessionConsumerMap =  new HashMap<>();
    private CloseableHttpClient httpClient = HttpClients.createDefault();
	private String hostname = "http://promenade-frontend-promenade-lyon.apps.kube.rcost.unisannio.it";
	
	
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
		      
		System.out.println("OnMessage : "+message);
		String[] cornerArr =  message.split(",");

		System.out.println("UpperLeftLon: " + Float.parseFloat(cornerArr[0]));
        System.out.println("UpperLeftLat: " + Float.parseFloat(cornerArr[1]));
        System.out.println("LowerRightLon: " + Float.parseFloat(cornerArr[2]));
        System.out.println("LowerRightLat: " + Float.parseFloat(cornerArr[3]));
        
        String path= "/promenadeAreaNameService/rest/areaService/areas?upperLeft="+ cornerArr[0] +","+ cornerArr[1] +"&lowerRight="+ cornerArr[2] +","+ cornerArr[3];	

        //Create an HttpGet object
        HttpGet httpget = new HttpGet(hostname+path);

        //Execute the Get request
        CloseableHttpResponse httpresponse = httpClient.execute(httpget);

        try {
            String topicString = null;
            Scanner sc = new Scanner(httpresponse.getEntity().getContent());
            while(sc.hasNext()) {
                topicString = sc.nextLine();
                System.out.println("REST response: "+topicString);
             }
            
            sc.close();

            if(topicString != null){
                topicString = topicString.replace("\"","");
                topicString = topicString.replace("]","");
                topicString = topicString.replace("[","");
                String[] topicArr =  topicString.split(",");

                for(int i =0; i<topicArr.length;i++){
                    topicArr[i] = topicArr[i].concat("-Northbound");
                }
    
                List<String> topicList = Arrays.asList(topicArr);
    

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
               
        } finally {
            httpresponse.close();
        }
        	
    }


    @OnClose
    public void onClose(Session session) throws IOException, EncodeException {
    	System.out.println("OnClose !! ");
		this.sessionConsumerMap.get(session).shutdown();
    	this.sessionConsumerMap.remove(session);
		this.sessionTopicMap.remove(session);

        session.close();
        httpClient.close();
    }

    @OnError
    public void onError(Session session, Throwable throwable) throws IOException {
        // Do error handling here
    	System.out.println("OnError !! ");
    	this.sessionConsumerMap.get(session).shutdown();
    	
		this.sessionConsumerMap.remove(session);
		this.sessionTopicMap.remove(session);
    	session.close();
        httpClient.close();
    	
    	System.out.println(throwable.getCause());
    	System.out.println(throwable.getMessage());
    	throwable.printStackTrace();
    }
    
    
}
