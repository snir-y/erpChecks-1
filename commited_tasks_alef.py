# import sys
import os
# import inspect
# import pkgutil
# from pathlib import Path
# from importlib import import_module

from datetime import datetime, timedelta
from time import sleep
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
try:
    import erpChecks.tools as tools
except ImportWarning:
    import tools


# =============== set start_date and configuration variables =================
default_args = {
    'owner': 'Snir',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

default_args['faas_url'] = "http://" + os.environ['LOCAL_SRV_IP']+ ":8080"   # 'http://regbais:8080'   # prod
default_args['d_con'] = {'DB_NAME': 'WiseERPRegba',
                         'DB_SRV': '172.16.11.5'}

default_args['dag_name'] = 'commited_tasks_alef'
default_args['dag_desc'] = 'checks commites tasks concerning master plan for production'

mail_name = 'mail_body_task_alef'
default_args['mail_subject'] = 'סריקת תוכנית סופית להזמנה - הודעה אוטומטית'
default_args['mail_body'] = getattr(mail_body, mail_name)
default_args['mail_footer'] = 'DAG: ERPChecks - ' + default_args['dag_name']


Managers = tools.connect_and_query(
                        qry=queries.qry_get_managers,
                        params=None,
                        d_con=default_args['d_con'], faas_url=default_args['faas_url'])

default_args['Managers'] = [(x['UserIndex'], x['EmployeeEmail']) for x in Managers]

# ============== Operators =================


def managerial(manager, subject, body, footer,  d_con, faas_url):
    manager_index = manager[0]
    manager_mail = manager[1]

    qry_result = tools.connect_and_query(
        qry=queries.qry_get_commited_task_alef_from_previous_day,
        params=[manager_index, None],  # getting all orders in branch
        d_con=d_con, faas_url=faas_url)

    desk_data = tools.connect_and_query(
        qry=queries.qry_get_roles_in_manager_branch,
        params=[manager_index, 14],  # 14=desk
        d_con=d_con, faas_url=faas_url)
    if desk_data is None or len(desk_data) == 0:
        desk_mails = []
    else:
        desk_mails = [x['EmployeeMail'] for x in desk_data]

    desk_mails.append(manager_mail)
    cc = ['snir-y@regba.co.il', 'heftsi-b@regba.co.il']

    ret = tools.send_mail_if_not_empty(
        mailadress_to=desk_mails,
        mailadress_cc=cc,
        subject=subject,
        body=body,
        qry_result=qry_result,
        footer=footer,
        faas_url=faas_url)
    return ret


def designerial(manager, subject, body, footer,  d_con, faas_url):
    manager_index = manager[0]
    manager_mail = manager[1]

    designer_data = tools.connect_and_query(
        qry=queries.qry_get_roles_in_manager_branch,
        params=[manager_index, 13],  # 13=designer
        d_con=default_args['d_con'], faas_url=default_args['faas_url'])
    if designer_data is None or len(designer_data) == 0:
        return None

    ret_list = []
    for designer in designer_data:
        designer_index = designer['EmployeeUserIndex']
        designer_mail = designer['EmployeeMail']

        sleep(1)

        qry_result = tools.connect_and_query(
            qry=queries.qry_get_commited_task_alef_from_previous_day,
            params=[None, designer_index],  # getting all orders in branch for specific designer
            d_con=d_con,
            faas_url=faas_url)

        sleep(10)

        ret = tools.send_mail_if_not_empty(
                mailadress_to=[designer_mail],
                mailadress_cc=[],  # 'snir-y@regba.co.il',
                subject=subject,
                body=body,
                footer=footer,
                qry_result=qry_result,
                faas_url=faas_url)
        ret_list.append((designer_index, ret))

    return ret

# ================ DAG ===================


dag = DAG(default_args['dag_name'],
          description=default_args['dag_desc'],
          default_args=default_args,
          schedule_interval='1 22 * * *',
          catchup=False)

with dag:
    start = DummyOperator(task_id='start')
    for manager in default_args['Managers']:

        branch_op = PythonOperator(
            task_id='branch_manager_{0}'.format(manager[0]),
            python_callable=managerial,
            dag=dag,
            op_args=[
                manager,
                default_args['mail_subject'],
                default_args['mail_body'],
                default_args['mail_footer'],
                default_args['d_con'],
                default_args['faas_url']],
            provide_context=False)

        designer_op = PythonOperator(
            task_id='designer_op_{0}'.format(manager[0]),
            python_callable=designerial,
            dag=dag,
            op_args=[
                manager,
                default_args['mail_subject'],
                default_args['mail_body'],
                default_args['mail_footer'],
                default_args['d_con'],
                default_args['faas_url']],
            provide_context=False)

        start >> branch_op >> designer_op
