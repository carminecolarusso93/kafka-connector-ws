from multiprocessing.connection import wait
from urllib import response
import websocket
import time
import requests

#open = False
upperLeft_x = 45.758767
upperLeft_y = 4.794102
lowerRight_x = 45.737867
lowerRight_y = 4.877417

upperLeft_x2 = 45.768
upperLeft_y2 = 4.8
lowerRight_x2 = 45.762
lowerRight_y2 = 4.82


addr = "ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector-geojson/"
#addr = "ws://localhost:8081/java-websocket/kafka-connector/"

def topics_string(response):
    
    if(response.status_code!=200):
        print("ERROR")
        return
        
    topics =  response.text
    

    print("Topics: "+topics[1:-1])
    topics = topics.replace('[','').replace(']','').replace('"','')
    listTopics =  topics.split(',')
    print("LIST: "+str(listTopics))

    nortBoundString=""
    for t in listTopics:
        nortBoundString= nortBoundString + t+'-Northbound,'
    
    nortBoundString =  nortBoundString[0:-1]
    print(nortBoundString)
    
    return nortBoundString


def on_message(ws, message):
    print(message + "\n")

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    print("### on_open ###")
    '''
    #hostname= 'http://promenadeareanameservice-promenade-lyon.apps.kube.rcost.unisannio.it'
    hostname= 'http://promenade-frontend-promenade-lyon.apps.kube.rcost.unisannio.it'
    path= '/promenadeAreaNameService/rest/areaService/areas?upperLeft='+str(upperLeft_x)+','+str(upperLeft_y)+'&lowerRight='+str(lowerRight_x)+','+str(lowerRight_y)
    
    print(path)
     
    response = requests.get(hostname+path)
    
    nortBoundString=topics_string(response)
    

    #ws.send(nortBoundString)
    '''
    #ws.send("Lyon_5e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound")
    #ws.send("Lyon_5e_Arrondissement,Lyon_9e_Arrondissement")
    ws.send("Lyon_1er_Arrondissement,Lyon_5e_Arrondissement,Lyon_9e_Arrondissement")
    #ws.send("Lyon_5e_Arrondissement")
    #time.sleep(3)
    '''
    path2= '/promenadeAreaNameService/rest/areaService/areas?upperLeft='+str(upperLeft_x2)+','+str(upperLeft_y2)+'&lowerRight='+str(lowerRight_x2)+','+str(lowerRight_y2)

    print("Second request")
    response = requests.get(hostname+path2)
    
    nortBoundString=topics_string(response)
    #ws.send(nortBoundString)
    #ws.send("Lyon_2e_Arrondissement-Northbound") 
    #ws.send("Lyon_1er_Arrondissement-Northbound,Lyon_5e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound")
    
    #start()
    '''

'''
def start():
    ws.send("Lyon_1er_Arrondissement-Northbound,Lyon_5e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound")
    
    #ws.run_forever()
    
    while True:
        input(send(message))
'''

    

def send(message):
    print("Message: "+message)
    ws.send(message)

if __name__ == "__main__":
    #websocket.enableTrace(True)
    ws = websocket.WebSocketApp(addr,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    
    
    #ws.send("Lyon_1er_Arrondissement-Northbound,Lyon_5e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound")
    ws.run_forever()
    #while(not open):
    #    wait(1000)