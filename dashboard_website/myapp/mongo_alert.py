from pymongo import MongoClient
from bson.objectid import ObjectId

class MongoAlert():
	def __init__(self):
		self.db = MongoClient('mongodb://localhost:27017/')['alerts'].alerts

	def __replace_id(self, alert):
		alert['id'] = alert['_id'] # Django template system doesn't accept field names starting with _
		del alert['_id']

	def get_alert(self, alert_id):
		alert = self.db.find_one({'_id':ObjectId(alert_id)})
		self.__replace_id(alert)
		return alert

	def get_all_alerts(self):
		alerts = list(self.db.find())
		for alert in alerts:
			self.__replace_id(alert)
		return alerts

	def delete_alert(self, alert_id):
		self.db.remove({'_id':ObjectId(alert_id)})