import sqlite3
import sys
import os

basepath = os.path.abspath(os.path.dirname(__file__))

conn = sqlite3.connect(os.path.join(basepath,'airflow.db'))
c = conn.cursor()

dag_input = sys.argv[1]

for t in ["xcom", "task_instance", "sla_miss", "log", "job", "dag_run", "dag" ]:
    query = "delete from {} where dag_id='{}'".format(t, dag_input)
    c.execute(query)

conn.commit()
conn.close()