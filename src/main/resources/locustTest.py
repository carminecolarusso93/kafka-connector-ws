from locust import HttpUser, TaskSet, task, between,events, constant
import websocket
from websocket import create_connection
import time
import random
import gevent

from uuid import uuid4

'''
python3 -m locust -f locustTest.py --host ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector/ --users 100 --spawn-rate 100 --run-time 1m --headless --html reports/8marzo/report-BASE-100user-100spawnrate-1pod-4core-1minutes.html
'''
class UserBehavior(TaskSet):

    def on_start(self):
        uri = "ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector/"
        #websocket.enableTrace(True)
        self.ws = create_connection(uri)
        self.ws.send("Lyon_1er_Arrondissement-Northbound,Lyon_5e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound")
        
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
        
        self.ws.send("Lyon_8e_Arrondissement-Northbound,Lyon_1er_Arrondissement-Northbound,Lyon_4e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound")
        #self.ws.send("Lyon_8e_Arrondissement-Northbound")
        
        events.request_success.fire(
            request_type='WebSocket Sent',
            name='test/ws/connector',
            response_time=int((time.time() - start_at) * 1000000),
            response_length=len("Lyon_8e_Arrondissement-Northbound,Lyon_1er_Arrondissement-Northbound,Lyon_4e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound")
            #response_length=len("Lyon_8e_Arrondissement-Northbound")
        )

   

    
    @task
    def other_message2(self):
        start_at = time.time()
        
        #self.environment.runner.quit()
        
        self.ws.send("Lyon_3e_Arrondissement-Northbound,Lyon_2e_Arrondissement-Northbound")
        
        
        events.request_success.fire(
            request_type='WebSocket Sent',
            name='test/ws/connector',
            response_time=int((time.time() - start_at) * 1000000),
            response_length=len("Lyon_3e_Arrondissement-Northbound,Lyon_2e_Arrondissement-Northbound")
        )
    
       
class WebsiteUser(HttpUser):

    tasks = [UserBehavior]
    wait_time = constant(10)