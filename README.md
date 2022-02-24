# Kafka sniffer

Initially, this tool was build for internal usage: we had a shared Kafka cluster with thousands topics and hundreds
clients (without any auth) and wanted to migrate all this stuff to the new cluster. For this reason, we needed to know
what mircoservices are still connected to the old cluster and what topics this service uses.

This sniffer can:

- detect active connections to Kafka Broker and can say who is producer and who is consumer
- detect topics to which producers trying to write / consumers trying to read
- expose IPs, request kind and topic as Prometheus metrics

Kafka protocol: https://kafka.apache.org/protocol

## changes

1. 2022-02-24 add `/client` API to get the result of client statistics, See the [demo](#demo1client).
2. 2022-02-24 Print clients of `kafka.ProduceRequest` and `kafka.FetchRequest`, see the [demo](#demo1).

## example

Example:

```
// Run simple producer who writes to topic "mytopic"
go run cmd/producer/main.go -brokers 127.0.0.1:9092

// Run sniffer on net iface (loopback or usually, eth0)
go run cmd/sniffer/main.go -i=lo0

// OR with debug info:
go run cmd/sniffer/main.go -i=lo0 -assembly_debug_log=false
```

Example output:

```
2020/05/16 16:25:49 got request, key: 0, version: 0, correlationID: 132, clientID: sarama
2020/05/16 16:25:49 client 127.0.0.1:60423 wrote to topic mytopic
2020/05/16 16:25:54 got request, key: 0, version: 0, correlationID: 133, clientID: sarama
2020/05/16 16:25:54 client 127.0.0.1:60423 wrote to topic mytopic
2020/05/16 16:25:59 got request, key: 0, version: 0, correlationID: 134, clientID: sarama
2020/05/16 16:25:59 client 127.0.0.1:60423 wrote to topic mytopic
2020/05/16 16:26:04 got request, key: 0, version: 0, correlationID: 135, clientID: sarama
2020/05/16 16:26:04 client 127.0.0.1:60423 wrote to topic mytopic
2020/05/16 16:26:05 got EOF - stop reading from stream
```

## Run as a Docker container

```
docker build . -t kafka-sniffer
docker run --rm --network host kafka-sniffer:latest -i lo0
```

## Run Kafka in docker (bitnami Kafka + Zookeeper)

```
docker-compose ./etc/docker-compose.yml up
```

## Run Kafka in minicube (Strimzi Kafka Operator)

```yaml
kubectl create namespace kafka
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
```

Send & receive messages

```yaml
kubectl -n kafka run kafka-producer -ti --image=strimzi/kafka:0.17.0-kafka-2.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --broker-list my-cluster-kafka-bootstrap:9092 --topic my-topic
kubectl -n kafka run kafka-consumer -ti --image=strimzi/kafka:0.17.0-kafka-2.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic --from-beginning
```

Port-forwarding for local development:

```yaml
kubectl port-forward service/my-cluster-kafka-brokers 9092
```

And probably you'll need to add this row to `/etc/hosts`

```yaml
127.0.0.1   my-cluster-kafka-0.my-cluster-kafka-brokers.kafka.svc
```

## demo1

```sh
# kafka-sniffer
2022/02/24 13:34:42 starting capture on interface "eth0"
2022/02/24 13:34:42 client 192.1.1.15:61285-192.1.1.14:9092 type: *kafka.FetchRequest topic [dev-logcenter], correlationID: 117377425, clientID: sarama
2022/02/24 13:34:42 client 192.1.1.15:37953-192.1.1.14:9092 type: *kafka.ProduceRequest topic [dev-metrics], correlationID: 6003063, clientID: sarama
2022/02/24 13:34:42 client 192.1.1.11:24717-192.1.1.14:9092 type: *kafka.FetchRequest topic [dev-metrics], correlationID: 196489671, clientID: sarama
2022/02/24 13:34:42 client 192.1.1.7:37233-192.1.1.14:9092 type: *kafka.FetchRequest topic [__consumer_offsets], correlationID: 247189, clientID: consumer-KMOffsetCache-cmak-548974c6c4-sxvgt-1723
2022/02/24 13:34:42 client 192.1.6.17:51404-192.1.1.14:9092 type: *kafka.FetchRequest topic [dev-ids], correlationID: 6716609, clientID: consumer-1
2022/02/24 13:34:42 client 192.1.9.23:36866-192.1.1.14:9092 type: *kafka.FetchRequest topic [bq_disaster_recovery], correlationID: 623626, clientID: consumer-1
2022/02/24 13:34:42 client 192.1.1.7:34038-192.1.1.14:9092 type: *kafka.FetchRequest topic [agent_transaction], correlationID: 12480162, clientID: consumer-1
2022/02/24 13:34:42 client 192.1.1.4:55214-192.1.1.14:9092 type: *kafka.FetchRequest topic [dev-cloudSignLogServer], correlationID: 3341672, clientID: 2428545257036493
2022/02/24 13:34:42 client 192.1.1.12:6267-192.1.1.14:9092 type: *kafka.FetchRequest topic [judicial_disaster], correlationID: 9009620, clientID: consumer-2
2022/02/24 13:34:42 client 192.1.1.11:33378-192.1.1.14:9092 type: *kafka.ProduceRequest topic [dev-gateway], correlationID: 10948681, clientID: producer-1
2022/02/24 13:34:42 client 192.1.1.12:9195-192.1.1.14:9092 type: *kafka.FetchRequest topic [judicial-2tripartite], correlationID: 9011202, clientID: consumer-1
2022/02/24 13:34:42 client 192.1.1.12:41426-192.1.1.14:9092 type: *kafka.FetchRequest topic [agent_count_transaction], correlationID: 194647, clientID: consumer-11
2022/02/24 13:34:42 client 192.1.1.11:22615-192.1.1.14:9092 type: *kafka.FetchRequest topic [ids-message-record-1], correlationID: 8999184, clientID: consumer-1
2022/02/24 13:34:42 client 192.1.1.12:20394-192.1.1.14:9092 type: *kafka.FetchRequest topic [count_transaction_pro], correlationID: 3240311, clientID: consumer-11
2022/02/24 13:34:42 client 192.1.1.12:7273-192.1.1.14:9092 type: *kafka.FetchRequest topic [transaction_pro], correlationID: 3240395, clientID: consumer-1
2022/02/24 13:34:42 client 192.1.1.4:6654-192.1.1.14:9092 type: *kafka.FetchRequest topic [count_transaction], correlationID: 572423, clientID: consumer-1
2022/02/24 13:34:42 client 192.1.1.4:48249-192.1.1.14:9092 type: *kafka.FetchRequest topic [transaction], correlationID: 8692411, clientID: consumer-11
2022/02/24 13:34:42 client 192.1.9.23:33500-192.1.1.14:9092 type: *kafka.FetchRequest topic [verif_supplement_file_v1], correlationID: 117992, clientID: consumer-2
```

### demo1client

```sh
# gurl :9870/client
Conn-Session: 127.0.0.1:44828->127.0.0.1:9870 (reused: false, wasIdle: false, idle: 0s)
GET /client? HTTP/1.1
Host: 127.0.0.1:9870
Accept: application/json
Accept-Encoding: gzip, deflate
Content-Type: application/json
Gurl-Date: Thu, 24 Feb 2022 06:30:03 GMT
User-Agent: gurl/1.0.0


HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Date: Thu, 24 Feb 2022 06:30:03 GMT

[
  {
    "Start": "2022-02-24T14:29:19.049701376+08:00",
    "Client": "192.1.6.17:51404",
    "ReqType": "*kafka.FetchRequest",
    "ClientID": "consumer-1",
    "Requests": 89,
    "BytesRead": 7387,
    "Topics": [
      "dev-ids"
    ]
  },
  {
    "Start": "2022-02-24T14:29:19.025437041+08:00",
    "Client": "192.1.8.12:6267",
    "ReqType": "*kafka.FetchRequest",
    "ClientID": "consumer-2",
    "Requests": 89,
    "BytesRead": 7031,
    "Topics": [
      "judicial_disaster"
    ]
  },
  {
    "Start": "2022-02-24T14:29:20.301435997+08:00",
    "Client": "192.1.6.15:56324",
    "ReqType": "*kafka.ProduceRequest",
    "ClientID": "sarama",
    "Requests": 309,
    "BytesRead": 123625,
    "Topics": [
      "dev-metrics"
    ]
  },
  {
    "Start": "2022-02-24T14:29:20.84427283+08:00",
    "Client": "192.1.6.4:54598",
    "ReqType": "*kafka.ProduceRequest",
    "ClientID": "sarama",
    "Requests": 283,
    "BytesRead": 113472,
    "Topics": [
      "dev-metrics"
    ]
  }
]
```