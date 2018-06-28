"""
Module to communicate with the Flink server running locally to do tasks like uploading an alert's jar, running the jar as a Flink job,
cancelling a running job and checking the status of jobs.
"""

import requests, os, json

class FlinkAPI:
	"""
	Class to communicate with Flink server.
	"""
	def __init__(self, hostname="localhost", port=8081):
		self.base_url = "http://%s:%d/"%(hostname,port)


	def upload_jar(self, jar_path):
		"""
		Uploads the jar of the alert to the Flink server running locally.

		:param jar_path: Path of the jar file to upload.
		:returns: The jar_id as returned by Flink server.
		"""
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
		"""
		Runs the given jar_id on Flink.

		:param alert_name: Name of the alert to which the jar_id belongs.
		:param flink_jar_id: jar_id to be run on Flink.
		:returns: The job_id of the Flink job started as returned by the Flink server.
		"""
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
		"""
		Cancels the Flink job with the given job_id.

		:param job_id: job_id of the Flink job to cancel.
		"""
		try:
			r = requests.delete(self.base_url + "jobs/%s/cancel"%job_id)
			r.raise_for_status()
			print('Response from cancel_job: %s'%(r.text)) # it is successful probably only if r.text = '{}'
			# response = json.loads(r.text)
			# return response == {}
		except Exception as e:
			raise Exception('Failed to pause job. Error: %s, %s'%(str(type(e)),str(e)))


	def check_job_status_all(self):
		"""
		Check the status of all jobs run in the past.

		:returns: Dictionary having key as alert_name (which was supplied in run_jar) and value as status of the last job of that alert.
		"""
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
