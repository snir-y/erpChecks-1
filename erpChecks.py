from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os
import requests
import json

try:
    from erpChecks.queries import qry_get_managers, qry_get_orders_with_bad_designer_mashlim
except ImportWarning :
    from queries import qry_get_managers, qry_get_orders_with_bad_designer_mashlim
try:
    from erpChecks.mail_body import mail_body_mashlim
except ImportWarning:
    from mail_body import mail_body_mashlim

default_args = {
    'owner': 'Snir',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

faas_url = 'http://' + os.environ['LOCAL_SRV_IP'] + ':8080'  # prod
# faas_url = http://172.16.11.89:8080 #test
d_con = {'DB_NAME': 'WiseERPRegba',
         'DB_SRV': '172.16.11.5'}


def get_managers():
    query_data = {'d_con': d_con,
                  'qry': qry_get_managers}
    r = requests.post('{}/function/mssql'.format(faas_url),
                      data=json.dumps(query_data))
    qry_result = r.json()
    return qry_result['data_result']


Managers = get_managers()
Managers = [(x['UserIndex'], x['EmployeeEmail']) for x in Managers]
default_args['Managers'] = Managers


def connect_and_query(userindex):
    query_data = {'d_con': d_con,
                  'qry': qry_get_orders_with_bad_designer_mashlim,
                  'qry_params': [userindex],
                  'lower_keys': False}
    r = requests.post('{}/function/mssql'.format(faas_url),  # prod
                      data=json.dumps(query_data))
    if r.status_code != 200:
        return None
    qry_result = r.json()
    if qry_result is None:
        return None

# print(qry_result)
    return qry_result['data_result']


def send_mail_if_not_empty(userindex, mailadress, **context):
    qry_result = context['task_instance'].xcom_pull(task_ids='connect_and_query_{0}'.format(userindex))
    if qry_result is None or len(qry_result) == 0:
        return None

    json2tbl_url = '{}/function/json2tbl'.format(faas_url)  # prod
    req = {'table': qry_result}
    r_table = requests.post(json2tbl_url, data=json.dumps(req))
    # refactor this
    mail_url = '{}/function/mail'.format(faas_url)  # prod
    mail_data = {
        'recipient': [mailadress],
        'cc': ['snir-y@regba.co.il', 'lior-r@regba.co.il'],
        'subject': 'בחירת מעצבת לא תקינה בהזמנות משלימים - הודעה אוטומטית ',
        # notice mail_body_mashlim is global
        'content': mail_body_mashlim + r_table.content.decode('utf-8'),
        'attachments': [],
        'footer': 'DAG: ERPChecks -  check_designer_mashlimim'
    }
    mail_data['title'] = mail_data['subject']

    r_mail = requests.post(mail_url, data=json.dumps(mail_data))
    return r_mail.status_code


dag = DAG('check_designer_mashlimim',
          description='checks the choice of designer in orders of type mashlimim',
          default_args=default_args,
          schedule_interval='1 9 * * 0',
          catchup=False)

with dag:
    start = DummyOperator(task_id='start')
    for manager in default_args['Managers']:

        connectNquery_operator = PythonOperator(task_id='connect_and_query_{0}'.format(manager[0]), retries=3,
                                                python_callable=connect_and_query, op_kwargs={'userindex': manager[0]}, dag=dag)

        mail_operator = PythonOperator(task_id='send_mail_if_not_empty_{0}'.format(manager[0]),
                                       python_callable=send_mail_if_not_empty, dag=dag, op_args=[manager[0], manager[1]], provide_context=True)

        start >> connectNquery_operator >> mail_operator
