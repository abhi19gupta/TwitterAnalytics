import requests, os, json

class FlinkAPI:

	def __init__(self, hostname="localhost", port=8081):
		self.base_url = "http://%s:%d/"%(hostname,port)

	def upload_jar(self, jar_path):
		try:
			r = requests.post(
				self.base_url + "jars/upload",
				files = {
					"jarfile": (
						os.path.basename(jar_path),
						open(jar_path, "rb"),
						"application/x-java-archive"
					)
				}
			)
			r.raise_for_status()
			print('Response from upload_jar: %s'%(r.text))
			response = json.loads(r.text)
			flink_jar_id = response["filename"]
			return flink_jar_id
		except Exception as e:
			raise Exception('Failed to upload jar. Error: %s, %s'%(str(type(e)),str(e)))

	def run_jar(self, alert_name, flink_jar_id):
		try:
			r = requests.post(self.base_url + "jars/%s/run?entry-class=%s&program-args=%s"%(flink_jar_id, "org.myorg.quickstart.StreamingJob",alert_name))
			r.raise_for_status()
			print('Response from run_jar: %s'%(r.text))
			response = json.loads(r.text)
			job_id = response["jobid"]
			return job_id
		except Exception as e:
			raise Exception('Failed to run jar. Error: %s, %s'%(str(type(e)),str(e)))

	def cancel_job(self, job_id):
		try:
			r = requests.delete(self.base_url + "jobs/%s/cancel"%job_id)
			r.raise_for_status()
			print('Response from cancel_job: %s'%(r.text)) # it is successful probably only if r.text = '{}'
			# response = json.loads(r.text)
			# return response == {}
		except Exception as e:
			raise Exception('Failed to pause job. Error: %s, %s'%(str(type(e)),str(e)))

	def check_job_status_all(self):
		try:
			# r = requests.get(self.base_url + "jobs")
			r = requests.get(self.base_url + "joboverview")
			r.raise_for_status()
			# print('Response from check_job_status_all: %s'%(r.text))
			response = json.loads(r.text)
			ret = {}
			# put the latest status in the ret
			for x in response['finished']: 
				alert_name = x['name']
				latest_time_seen = float('inf') if alert_name not in ret else ret[alert_name]['start-time']
				if x['start-time'] < latest_time_seen:
					ret[alert_name] = {'start-time':x['start-time'], 'status':x['state']}
			# remove the start-time as we need just the state
			for alert_name in ret:
				ret[alert_name] = 'FINISHED - ' + ret[alert_name]['status']
			for x in response['running']: 
				ret[x['name']] = 'RUNNING'
			return ret
		except Exception as e:
			raise Exception('Failed to check job statuses. Error: %s, %s'%(str(type(e)),str(e)))