rm bg_procs/*
echo "** Removed previous procs info **\n"

sudo service mongod start
echo "** Started mongod service **\n"

Flink/flink-1.4.2/bin/start-local.sh
echo "** Started flink server **\n"

nohup Kafka/kafka_2.11-1.1.0/bin/zookeeper-server-start.sh Kafka/kafka_2.11-1.1.0/config/zookeeper.properties > bg_procs/kafka_zoo.out 2>&1 &
echo $! > bg_procs/kafka_zoo.pid # writes the pid of the previous command, to be used later to kill it.
echo "** Started Kafka zookeeper **\n"

echo "** Sleeping for 5 secs **\n"
sleep 5

nohup Kafka/kafka_2.11-1.1.0/bin/kafka-server-start.sh Kafka/kafka_2.11-1.1.0/config/server.properties > bg_procs/kafka_server.out 2>&1 &
echo $! > bg_procs/kafka_server.pid
echo "** Started Kafka server **\n"

echo "** Sleeping for 15 secs **\n"
sleep 15

Kafka/kafka_2.11-1.1.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tweets_topic
echo "** Created kafka topic - tweets_topic **\n"

echo "** Sleeping for 5 secs **\n"
sleep 5

# nohup python Kafka/flink_kafka_consumer.py > bg_procs/flink_kafka_consumer.out 2>&1 &
# echo $! > bg_procs/flink_kafka_consumer.pid
# echo "** Started flink_kafka_consumer **\n"

echo "** Following Kafka topics exist:"
Kafka/kafka_2.11-1.1.0/bin/kafka-topics.sh --list --zookeeper localhost:2181