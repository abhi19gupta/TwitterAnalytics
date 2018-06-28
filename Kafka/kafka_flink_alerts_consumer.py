"""
Module to read alerts from the Kafka topic ("alerts_topic") where Flink applications post alerts and then put them in MongoDB.
"""

from kafka import KafkaConsumer, TopicPartition
import json, pymongo, datetime
from pymongo import MongoClient

def insert_records(records):
	for record in records:
		# No need to display alerts for empty keys
		if (record['key'] in ['{"hashtag":"__NO_HASHTAG_FOUND__"}', '"url":"__NO_URLS_FOUND__"', {'"user_mention":"__NO_MENTIONS_FOUND__"'}]):
			continue
		record['window_start'] = datetime.datetime.fromtimestamp(record['window_start']/1000)
		record['window_end'] = datetime.datetime.fromtimestamp(record['window_end']/1000)
		db.update({'alert_name':record['alert_name'], 'window_start':record['window_start'], 'window_end':record['window_end'], 
			'key':record['key']}, record, upsert=True)	

if __name__ == "__main__":

	db = MongoClient('mongodb://localhost:27017/')['alerts'].alerts
	consumer = KafkaConsumer(group_id='flink_out_consumers', value_deserializer=json.loads)
	partition = TopicPartition('alerts_topic',0)
	consumer.assign([partition])
	print(consumer.assignment())
	# consumer.seek_to_beginning()

	while(True):
		try:
			ret = consumer.poll(10000) # polls for 10 seconds
			if ret != {}:
				records = [record.value for record in ret[partition]]
				print(records)
				insert_records(records)
			print('Ended poll')
		except Exception as e:
			print(e)
			continue	