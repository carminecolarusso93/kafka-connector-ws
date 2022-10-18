from locust import HttpUser, TaskSet, constant, task, between,events
import websocket
from websocket import create_connection
import time
import random
import gevent

from uuid import uuid4

'''
python3 -m locust -f locustMultipleGeoJsonREST.py --host ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector-rest-geojson/ --users 50 --spawn-rate 50 --run-time 1m --headless --html reports/15marzo/report-REST-GEOJSON-50user-50spawnrate-1pod-2NeoPod-1minutes.html

'''

class UserBehavior(TaskSet):

    def on_start(self):
        uri = "ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector-rest-geojson/"
        #websocket.enableTrace(True)
        self.ws = create_connection(uri)
        self.ws.send("Lyon_1er_Arrondissement,Lyon_5e_Arrondissement,Lyon_9e_Arrondissement")
        
        def _receive():
            
            countRc=0

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
                countRc=countRc+1
                #print('NUMERO RISPOSTA: '+str(countRc))
                

        gevent.spawn(_receive)

        
    def on_quit(self):
        self.ws.disconnect()

    

    @task
    def other_message1(self):
        
        start_at = time.time()
        
        #self.environment.runner.quit()
        
        self.ws.send("Lyon_8e_Arrondissement,Lyon_1er_Arrondissement,Lyon_4e_Arrondissement,Lyon_9e_Arrondissement")
        #self.ws.send("Lyon_8e_Arrondissement")
        
        events.request_success.fire(
            request_type='WebSocket Sent',
            name='test/ws/connector',
            response_time=int((time.time() - start_at) * 1000000),
            response_length=len("Lyon_8e_Arrondissement,Lyon_1er_Arrondissement,Lyon_4e_Arrondissement,Lyon_9e_Arrondissement")
            #response_length=len("Lyon_8e_Arrondissement")
        )

   

    
    @task
    def other_message2(self):
        start_at = time.time()
        
        #self.environment.runner.quit()
        
        self.ws.send("Lyon_3e_Arrondissement,Lyon_2e_Arrondissement")
        
        
        events.request_success.fire(
            request_type='WebSocket Sent',
            name='test/ws/connector',
            response_time=int((time.time() - start_at) * 1000000),
            response_length=len("Lyon_3e_Arrondissement,Lyon_2e_Arrondissement")
        )
    
       
class WebsiteUser(HttpUser):

    tasks = [UserBehavior]
    wait_time = constant(10)