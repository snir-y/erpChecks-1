import sys, os
import inspect
import pkgutil
from pathlib import Path
from importlib import import_module

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import requests
import json

try:
    import erpChecks.queries as queries
except ImportWarning:
    import queries
try:
    import erpChecks.mail_body as mail_body
except ImportWarning:
    import mail_body


# =============== set start_date and configuration variables ===================
default_args = {
    'owner': 'Snir',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


default_args['faas_url'] = 'http://'+ os.environ['LOCAL_SRV_IP'] +':8080'   # prod
default_args['d_con'] = {'DB_NAME': 'WiseERPRegba',
                        'DB_SRV': '172.16.11.5'}
default_args['dag_name'] = 'commited tasks alef'
default_args['dag_desc'] = 'checks commites tasks concerning master plan for production'

qry_name = 'qry_get_commited_task_alef_from_previous_day'
default_args['qry_check'] = getattr(queries, qry_name)


mail_name = 'mail_body_task_alef'
default_args['mail_subject'] = 'סריקת תוכנית סופית להזמנה - הודעה אוטומטית'
default_args['mail_body'] = getattr(mail_body, mail_name)


Managers = tools.connect_and_query(userindex = None, qry=queries.qry_get_managers, d_con=d_con, faas_url=faas_url)
Managers = [(x['UserIndex'], x['EmployeeEmail']) for x in Managers]
default_args['Managers'] = Managers


# ============= operators =================

def transaction(userindex, mailadress, subject, body, qry, d_con, faas_url):
    qry_result = tools.connect_and_query(userindex=userindex, qry=qry, d_con=d_con, faas_url=faas_url)
    ret = send_mail_if_not_empty(userindex=userindex, mailadress=mailadress, subject=subject,
                                body=body, qry_result= qry_result, faas_url=faas_url)





# ================ DAG ===================



dag = DAG(dag_name,
          description= dag_desc,
          default_args= default_args,
          schedule_interval='1 1 * * 0',
          catchup=False)

with dag:
    start = DummyOperator(task_id='start')
    for manager in default_args['Managers']:

        mail_and_connets_N_query_operator = PythonOperator(task_id='send_mail_if_not_empty_{0}'.format(manager[0]),
                                       python_callable=send_mail_if_not_empty, dag=dag, op_args=[manager[0], manager[1], mail_subject, mail_body_content], provide_context=False)

        start >> mail_and_connets_N_query_operator
