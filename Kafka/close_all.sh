kill -2 `cat bg_procs/flink_kafka_consumer.pid`
echo "** Killed Kafka consumer **\n"

echo "** Sleeping for 2 secs **\n"
sleep 2

Kafka/kafka_2.11-1.1.0/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic tweets_topic
Kafka/kafka_2.11-1.1.0/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic alerts_topic
echo "** Deleted Kafka topics - tweets_topic, alerts_topic **\n"

kill -2 `cat bg_procs/kafka_server.pid`
echo "** Killed Kafka server **\n"

echo "** Sleeping for 10 secs **\n"
sleep 10

kill -2 `cat bg_procs/kafka_zoo.pid`
echo "** Killed Kafka zoo **\n"

Flink/flink-1.4.2/bin/stop-local.sh
echo "** Killed Flink server **\n"
