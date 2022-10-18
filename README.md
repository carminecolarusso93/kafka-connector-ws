Connettore Kafka-Websocket

Comando per locust: python3 -m locust -f test.py --host ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector/ --users 300 --spawn-rate 300 --run-time 20s --headless --html /home/zimeo/Desktop/LocustReports/report.html


python3 -m locust -f locustRest.py --host ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector-rest-coordinates/ --users 50 --spawn-rate 50 --run-time 5m --headless --html reports/25febbraio/report-REST-50user-50spawnrate-1pod-5minutes.html



iperf -c 192.168.250.10 --port 80
------------------------------------------------------------
Client connecting to 192.168.250.10, TCP port 80
TCP window size:  135 KByte (default)
------------------------------------------------------------
[  3] local 10.10.0.5 port 37418 connected with 192.168.250.10 port 80
[ ID] Interval       Transfer     Bandwidth
[  3]  0.0-10.0 sec   108 MBytes  90.6 Mbits/sec
