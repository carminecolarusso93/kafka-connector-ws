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


addr = "ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector-rest-coordinates/"

def on_message(ws, message):
    print(message + "\n")

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    print("### on_open ###")
    time.sleep(3)
    ws.send(str(upperLeft_x) + "," + str(upperLeft_y) + "," + str(lowerRight_x) + "," + str(lowerRight_y))
    

if __name__ == "__main__":
    #websocket.enableTrace(True)
    ws = websocket.WebSocketApp(addr,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    
    ws.run_forever()
    