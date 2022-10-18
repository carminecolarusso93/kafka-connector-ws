from locust import HttpUser, TaskSet, task, between,events, constant
import websocket
from websocket import create_connection
import time
import random
import gevent

from uuid import uuid4

upperLeft_x = 45.758767
upperLeft_y = 4.794102
lowerRight_x = 45.737867
lowerRight_y = 4.877417

upperLeft_x2 = 45.768
upperLeft_y2 = 4.8
lowerRight_x2 = 45.762
lowerRight_y2 = 4.82


'''
python3 -m locust -f locustMongo.py --host ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector-coordinates/ --users 200 --spawn-rate 50 --run-time 1m --headless --html reports/23_03_2022/PROVA2-report-MONGO-200user-50spawnrate-2pod-1minutes.html

'''

class UserBehavior(TaskSet):

    def on_start(self):
        uri = "ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector-coordinates/"
        #websocket.enableTrace(True)
        self.ws = create_connection(uri)
        self.ws.send(str(upperLeft_x) + "," + str(upperLeft_y) + "," + str(lowerRight_x) + "," + str(lowerRight_y))
        
        def _receive():
            
            while True:
                res = self.ws.recv()
                '''
                start_at = time.time()
                end_at = time.time()
                response_time = int((end_at - start_at) * 1000000)
                events.request_success.fire(
                    request_type='WebSocket Recv',
                    name='test/ws/chat',
                    response_time=response_time,
                    response_length=len(res),
                )
                '''
                #print(res)
                

        gevent.spawn(_receive)

        
    def on_quit(self):
        self.ws.disconnect()

    

    @task
    def other_message1(self):
        
        start_at = time.time()
        
        #self.environment.runner.quit()
        
        self.ws.send(str(upperLeft_x) + "," + str(upperLeft_y) + "," + str(lowerRight_x) + "," + str(lowerRight_y))
        
        events.request_success.fire(
            request_type='WebSocket Sent',
            name='test/ws/connector',
            response_time=int((time.time() - start_at) * 1000000),
            response_length=len(str(upperLeft_x) + "," + str(upperLeft_y) + "," + str(lowerRight_x) + "," + str(lowerRight_y))
            
        )

   

  
    @task
    def other_message2(self):
        start_at = time.time()
        
        #self.environment.runner.quit()
        
        self.ws.send(str(upperLeft_x2) + "," + str(upperLeft_y2) + "," + str(lowerRight_x2) + "," + str(lowerRight_y2))
        
        events.request_success.fire(
            request_type='WebSocket Sent',
            name='test/ws/connector',
            response_time=int((time.time() - start_at) * 1000000),
            response_length=len(str(upperLeft_x2) + "," + str(upperLeft_y2) + "," + str(lowerRight_x2) + "," + str(lowerRight_y2))
        )

       
class WebsiteUser(HttpUser):

    tasks = [UserBehavior]
    wait_time = constant(10)