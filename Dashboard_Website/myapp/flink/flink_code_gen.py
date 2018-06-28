"""
Module to generate Java code for Flink from the alert specification taken from user on the dashboard. You need to have jinja2 python module and Maven installed.
"""

from jinja2 import Template
import shutil, os
from subprocess import Popen, PIPE

class FlinkCodeGenerator:
	"""
	Class to translate alert specification given by user to Java code for Flink. It uses a template defined in
	flink_template.txt. The user provided specification is translated to Java code and placed at appropriate
	positions inside the template. This renders the complete Java file. The Java project is then created
	containing this file and compiled using Maven.
	"""
	def __init__(self):
		self.basepath = os.path.abspath(os.path.dirname(__file__)) # path of this file, basepath of entrire flink code
		self.template_path = os.path.join(self.basepath,'flink_template.txt') # path of the template file
		self.BASIC_INDENT = 7 # using tab indentation in Java code; the variable part of template starts at indentation of 7 tabs
		attr_array_template = Template(''+\
			'List<String> {{attr}} = new ArrayList<>();\n' + '\t'*self.BASIC_INDENT+\
			'for (final JsonNode node : jsonNode.get("entities").get("{{attr}}"))\n' + '\t'*self.BASIC_INDENT+\
			'	{{attr}}.add(node.get("{{field_name}}").asText());\n' + '\t'*self.BASIC_INDENT)
		self.hashtags_array_code = attr_array_template.render(attr='hashtags',field_name='text') # java code for hashtag array from tweet
		self.urls_array_code = attr_array_template.render(attr='urls',field_name='expanded_url') # java code for urls array from tweet
		self.mentions_array_code = attr_array_template.render(attr='user_mentions',field_name='id_str') # java code for mentions array from tweet


	def _get_filter_code(self, filter_string):
		"""
		Internal function used by the class to translate the "filter" specification to Java code to be placed inside the template.

		:param filter_string: Example -> user_id.equals("abc") && (hashtags.contains("h1") || hashtags.contains("h2"))
		:returns: Java code, for filtering the tweet stream, to be placed inside the template.
		"""
		ret = ''
		if ('user_id.equals' in filter_string):
			ret += 'String user_id = jsonNode.get("user").get("id_str").asText();\n' + '\t'*self.BASIC_INDENT
		if ('hashtags.contains' in filter_string):
			ret += self.hashtags_array_code
		if ('urls.contains' in filter_string):
			ret += self.urls_array_code
		if ('user_mentions.contains' in filter_string):
			ret += self.mentions_array_code

		if ret == '':
			if filter_string == '':
				return 'return true;'
			else:
				raise Exception('Invalid filter string! [%s]'%filter_string)
		else:
			ret += 'return '+filter_string+';'

		return ret


	def _get_duplication_code(self, keys):
		"""
		Internal function. A single tweet might belong to multiple groups (sub-streams) after grouping on keys because
		each key can have multiple values (a list). Ex. If keys = ['hashtag'] and tweet contains hashtags = ['h1','h2'],
		then this tweet belongs to 2 groups - one group for 'h1' and another for 'h2'. This function duplicates the tweet
		into multiple instances, each having its group value as the key. In above example, it will duplicate into 2 tweets,
		with key values as 'h1' and 'h2' respectively.

		:param keys: list of elements from 'user_id','hashtag','url','user_mention'
		:returns: Java code, for duplicating the tweet stream, to be placed inside the template.
		"""
		for key in keys:
			if key not in ['user_id','hashtag','url','user_mention']:
				raise Exception('Invalid keys: %s'%str(keys))
		ret = ''
		if 'user_id' in keys:
			ret += 'List<String> user_ids = new ArrayList<>();\n' + '\t'*self.BASIC_INDENT+\
				'user_ids.add(jsonNode.get("user").get("id_str").asText());\n\n' + '\t'*self.BASIC_INDENT
		if 'hashtag' in keys:
			ret += self.hashtags_array_code +\
				'if (hashtags.isEmpty())\n' + '\t'*self.BASIC_INDENT+\
				'	hashtags.add("__NO_HASHTAG_FOUND__");\n\n' + '\t'*self.BASIC_INDENT
		if 'url' in keys:
			ret += self.urls_array_code+\
				'if (urls.isEmpty())\n' + '\t'*self.BASIC_INDENT+\
				'	urls.add("__NO_URLS_FOUND__");\n\n' + '\t'*self.BASIC_INDENT

		if 'user_mention' in keys:
			ret += self.mentions_array_code+\
				'if (user_mentions.isEmpty())\n' + '\t'*self.BASIC_INDENT+\
				'	user_mentions.add("__NO_MENTIONS_FOUND__");\n\n' + '\t'*self.BASIC_INDENT

		for (i,key) in enumerate(keys):
			if key == 'user_id':
				ret += 'for (final String user_id : user_ids){\n' + '\t'*(self.BASIC_INDENT+i+1)
			elif key == 'hashtag':
				ret += 'for (final String hashtag : hashtags){\n' + '\t'*(self.BASIC_INDENT+i+1)
			elif key == 'url':
				ret += 'for (final String url : urls){\n' + '\t'*(self.BASIC_INDENT+i+1)
			elif key == 'user_mention':
				ret += 'for (final String user_mention : user_mentions){\n' +'\t'*(self.BASIC_INDENT+i+1)

		num_keys = len(keys)
		ret += 'ObjectNode newJsonNode = jsonNode.deepCopy();\n' + '\t'*(self.BASIC_INDENT+num_keys) +\
				'ObjectNode keyNode = newJsonNode.putObject("key");\n' + '\t'*(self.BASIC_INDENT+num_keys)
		for key in keys:
			ret += 'keyNode.put("%s",%s);\n'%(key,key) + '\t'*(self.BASIC_INDENT+num_keys)
		ret += 'out.collect(newJsonNode);'
		for i in range(num_keys):
			ret += '\n' + '\t'*(self.BASIC_INDENT+num_keys-i-1)+'}'
		return ret


	def _get_alert_base_path(self, alert_name):
		"""
		Internal function.

		:param alert_name: Name of the alert to find base path for.
		:returns: The base path for tje given alert's flink java project.
		"""
		return os.path.join(self.basepath, 'all_alerts', alert_name)


	def _get_alert_jar_path(self, alert_name):
		"""
		Internal function.

		:param alert_name: Name of the alert to find jar path for.
		:returns: The path of the jar file, created after compiling, for the given alert.
		"""
		alert_base_path = self._get_alert_base_path(alert_name)
		orig_jar_path = os.path.join(alert_base_path, 'target', 'quickstart-0.1.jar')
		if (not os.path.isfile(orig_jar_path)):
			return None
		new_jar_path = os.path.join(alert_base_path, '%s.jar'%alert_name)
		shutil.copyfile(orig_jar_path, new_jar_path)
		return new_jar_path


	def write_code(self, alert_name, filter_string, group_keys, window_length, window_slide, threshold):
		"""
		Generates the java code for the given alert specification and writes tha java project for it in the alert's base path.
		Note: It can raise exception like alert already exists with the given name.

		:param alert_name: Name of the alert to be created.
		:param filter_string: Filter specification for the alert. Refer to :func:`flink_code_gen.FlinkCodeGenerator._get_filter_code`.
		:param group_keys: List of keys to group on. Refer to :func:`flink_code_gen.FlinkCodeGenerator._get_duplication_code`.
		:param window_length: Length of window in seconds. The threshold will be looked at each window in each sub-stream.
		:param window_slide: Number of seconds after which to start each new window.
		:param threshold: Count threshold for tweets in each window to generate the alert.
		"""
		try:
			template_base_path = os.path.join(self.basepath,'quickstart')
			alert_base_path = self._get_alert_base_path(alert_name)
			shutil.copytree(template_base_path, alert_base_path)
			if os.path.exists(os.path.join(alert_base_path, 'target')):
				shutil.rmtree(os.path.join(alert_base_path, 'target'))

			filter_code = self._get_filter_code(filter_string)
			duplication_code = self._get_duplication_code(group_keys)
			template = Template(open(self.template_path,'r').read())
			java = template.render(filter_code=filter_code, duplication_and_key_generation_code=duplication_code,
				window_length=window_length, window_slide=window_slide, threshold=threshold)
			f = open(os.path.join(alert_base_path,'src','main','java','org','myorg','quickstart','StreamingJob.java'),'w')
			f.write(java)
			f.close()
		except Exception as e:
			raise Exception('Failed to write code. Error: %s, %s'%(str(type(e)),str(e)))


	def delete_code(self, alert_name):
		"""
		Deletes the java project for the given alert name.

		:param alert_name: The name of the alert whose code is to be deleted.
		"""
		alert_base_path = self._get_alert_base_path(alert_name)
		if os.path.exists(alert_base_path):
			shutil.rmtree(alert_base_path)


	def compile_code(self, alert_name):
		"""
		Compiles the java project for the given alert using maven and creates the jar file.

		:param alert_name: The name of the alert whose code is to be compiled.
		:returns: The path of the jar file resulting from the compilation.
		"""
		alert_base_path = self._get_alert_base_path(alert_name)
		orig_dir = os.getcwd()
		os.chdir(alert_base_path)
		try:
			childProc = Popen(['mvn clean package -Pbuild-jar'], stdout = PIPE, stderr=PIPE, bufsize=0, shell=True)
			(stdoutdata, stderrdata) = childProc.communicate() # waits for child to complete
			jar_path = self._get_alert_jar_path(alert_name)
			if jar_path==None:
				msg = str(stderrdata) + '. ' + str(stdoutdata)
				raise Exception(msg)
			else:
				return jar_path
		except Exception as e:
			raise Exception('Failed to compile code. Error: %s, %s'%(str(type(e)),str(e)))
		finally:
			os.chdir(orig_dir)

if __name__ == "__main__":
	gen = FlinkCodeGenerator()
	filter_string = 'user_id.equals("i") && (hashtags.contains("h") || urls.contains("u") || user_mentions.contains("m"))'
	group_keys = ['user_id','hashtag','url']
	# print(gen._get_filter_code(filter_string))
	# print(gen._get_duplication_code(group_keys))
	gen.write_code("my_alert",filter_string, group_keys, 10, 5, 3)
	print(gen.compile_code("my_alert"))
