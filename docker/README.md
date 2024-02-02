docker run \
-e ELASTIC_HOST=http://192.168.1.110:9200 \
-e REDIS_HOST=redis://192.168.1.110:6379 \
-e PULSAR_HOST=pulsar://192.168.1.110:6650 \
-e MODE=worker \
-e PAUSE=5 \
tracardi/apm:0.8.2.1
