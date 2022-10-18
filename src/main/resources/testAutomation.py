import os
import sys
import subprocess as subprocess
import time
from datetime import datetime


def test(locustPy, endpoint, client, spawnRate, times, directory, tipe, numPod):
    try:
        command_m = "python3 -m locust -f {} --host ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/{}/ --users {} --spawn-rate {} --run-time {} --headless --html reports/{}/report-{}-{}user-{}spawnrate-{}pod-4core-CASA-{}.html".format(
            locustPy, endpoint, client, spawnRate, times, directory, tipe, client, spawnRate, numPod, times)
        # command_w = "python3 -m locust -f {} --worker --master-host=127.0.0.1 --master-port=5557".format(
        #     locustPy, endpoint, client, spawnRate, directory, tipe, client, spawnRate, numPod, times)
        p_m = subprocess.Popen(command_m.split(), stdout=subprocess.PIPE)
        # p_s = subprocess.Popen(command_w.split())
        p_m.stdout.flush()
        # p_s.stdout.flush()
        # p_s.wait()
        p_m.wait()

        print("Done..")
    except Exception:
        print("Errore lancio {}".format(command_m))
        print(Exception.__name__)


def test2(scale):
    client = 50 * scale
    spawnRate = 50 * scale
    times = '2m'
    numPod = 1

    wait_Time_newTest = 180

    now = datetime.now()
    date_time = now.strftime("%d_%m_%Y")
    pathDir = 'reports/' + date_time
    try:
        os.makedirs(pathDir)
    except Exception:
        print("La cartella gia' esiste")
    
    print("start mongo")
    test('locustMongo.py', 'kafka-connector-coordinates', client, spawnRate, times, date_time, 'MONGO', numPod)
    print()
    print('---------------- FINE TEST ----------------------')
    print()
    time.sleep(wait_Time_newTest)
    
    
    print("start geo rest")
    test('locustMultipleGeoJsonREST.py', 'kafka-connector-rest-geojson', client, spawnRate, times, date_time,
         'REST-GEOJSON', numPod)
    print()
    print('---------------- FINE TEST ----------------------')
    print()
    time.sleep(wait_Time_newTest)

    
    print("start rest")
    test('locustRest.py', 'kafka-connector-rest-coordinates', client, spawnRate, times, date_time, 'REST', numPod)
    print()
    print('---------------- FINE TEST ----------------------')
    print()
    time.sleep(wait_Time_newTest)

    
    print("start base")
    test('locustTest.py', 'kafka-connector', client, spawnRate, times, date_time, 'BASE', numPod)
    print()
    print('---------------- FINE TEST ----------------------')
    print()

    
    time.sleep(wait_Time_newTest)
    '''
    test('locustGeoJson.py', 'kafka-connector-geojson', client, spawnRate, times, date_time, 'BASE-GEOJSON', numPod)
    print()
    print('---------------- FINE TEST ----------------------')
    print()
    time.sleep(wait_Time_newTest)
    '''
    print("start multiple geojson")
    client = 14 
    spawnRate = 14 
    test('locustMultipleGeoJson.py', 'kafka-connector-geojson-multiple', client, spawnRate, times, date_time, 'MULTIPLE-GEOJSON', numPod)
    print()
    print('---------------- FINE TEST ----------------------')
    print()
    


if __name__ == '__main__':

    for i in range(1,4):
        test2(i)
