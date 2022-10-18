import os
import sys
import subprocess as subprocess
import time
from datetime import datetime


def test(locustPy, endpoint, client, spawnRate, times, directory, tipe, numPod):
    try:
        command_m = "python3 -m locust -f {} --host ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/{}/ --users {} --spawn-rate {} --run-time {} --headless --html reports/{}/report-TOPICS-SCALING-{}-{}user-{}spawnrate-{}pod-{}.html".format(
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
    client = 1 * scale
    spawnRate = 1 * scale
    times = '3m'
    numPod = 1

    wait_Time_newTest = 90

    now = datetime.now()
    date_time = now.strftime("%d_%m_%Y")
    pathDir = 'reports/' + date_time
    try:
        os.makedirs(pathDir)
    except Exception:
        print("La cartella gia' esiste")
    

    print("start multiple-topic")
    test('locustTestAllTopics.py', 'kafka-connector', client, spawnRate, times, date_time, 'MULTIPLE-TOPICS', numPod)
    print()
    print('---------------- FINE TEST ----------------------')
    print()
    time.sleep(wait_Time_newTest)

    print("start single topic")
    test('locustTestOneTopic.py', 'kafka-connector', client, spawnRate, times, date_time, 'SINGLE-TOPIC', numPod)
    print()
    print('---------------- FINE TEST ----------------------')
    print()
   
    


if __name__ == '__main__':
    test2(1)