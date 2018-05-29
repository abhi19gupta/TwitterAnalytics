from __future__ import print_function
from airflow.operators import PythonOperator
from airflow.models import DAG
from datetime import datetime
import sys

sys.path.append("/home/db123/Documents/TwitterAnalytics/dashboard_website/myapp")
from views import execute

args = {
	'owner': 'airflow',
	'start_date': datetime.now(),
}

dag = DAG(dag_id='example', default_args=args,schedule_interval=None)

def execute_query(i):
	print(i)
	return 'print_context has sucess {}'.format(i)



task = PythonOperator(
		task_id='query.{}'.format("q6"),
		python_callable=execute,
		op_kwargs={'query_name':"q6","inputs":{"num":5}},
		dag=dag)
