# import sys
import os
# import inspect
# import pkgutil
# from pathlib import Path
# from importlib import import_module

from datetime import datetime, timedelta
from time import sleep
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests
import json
from erpChecks.queries import qry_get_commited_task_alef_from_previous_day, qry_get_managers, qry_get_roles_in_manager_branch
from erpChecks.mail_body import mail_body_task_alef




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

faas_url = "http://" + os.environ['LOCAL_SRV_IP']+ ":8080"   # 'http://regbais:8080'   # prod
d_con = {'DB_NAME': 'WiseERPRegba',
        'DB_SRV': '172.16.11.5'}

default_args['dag_desc'] = 'checks commits tasks concerning master plan for production'

mail_name = 'mail_body_task_alef'


def connect_and_query( qry, qry_params, d_con=d_con):
    query_data = {'d_con': d_con,
                'qry': qry,
                'qry_params': qry_params,  # [userindex],
                'lower_keys': False}
    r = requests.post('{}/function/mssql'.format(faas_url),  # prod
                    data=json.dumps(query_data))
    if r.status_code != 200:
        return None
    qry_result = r.json()
    if qry_result is None or 'data_result' not in qry_result.keys():
        return None
    # print(qry_result)
    return qry_result['data_result']

def send_mail_if_not_empty(to, cc, qry_result):
    if qry_result is None:
        return None
    json2tbl_url = '{}/function/json2tbl'.format(faas_url)  # prod
    req = {'table': qry_result}
    r_table = requests.post(json2tbl_url, data=json.dumps(req))
    # refactor this
    mail_url = '{}/function/mail'.format(faas_url)  # prod
    mail_data = {
        'recipient': to, # list
        'cc': cc, #['snir-y@regba.co.il', 'lior-r@regba.co.il'],
        'subject': 'הזמנות עם תאריך אספקה משוער לא תקין - הודעה אוטומטית ',
        'content': mail_body_task_alef + r_table.content.decode('utf-8'),
        'attachments': [],
        'footer': 'DAG: ERPChecks -  check_orders_with_old_est_supplydate'
    }
    mail_data['title'] = mail_data['subject']

    r_mail = requests.post(mail_url, data=json.dumps(mail_data))
    return r_mail.status_code


# ============== Operators =================

@dag(default_args=default_args, schedule_interval='1 22 * * *', start_date=days_ago(0))
def commited_tasks_alef():
    
    @task
    def get_managers():
        query_data = {'d_con': d_con,
                    'qry': qry_get_managers}
        r = requests.post('{}/function/mssql'.format(faas_url),
                        data=json.dumps(query_data))
        qry_result = r.json()
        return qry_result['data_result']
    
    @task()
    def get_orders_by_manager(managers):
        orders = {}
        for m in managers:
            data = connect_and_query(qry_get_commited_task_alef_from_previous_day, [m['UserIndex'], ''])  
            if data is None or len(data)==0:
                continue
            else:
                orders[m['EmployeeEmail']] = data
                desk_users = connect_and_query(qry_get_roles_in_manager_branch, [m['UserIndex'], 14]) # 14=desk
                if desk_users is None or len(desk_users) == 0:
                    desk_mails = []
                else:
                    desk_mails = [x['EmployeeMail'] for x in desk_users]           
                for x in desk_mails:
                    orders[x] = data
        return orders
    
    @task()
    def get_orders_by_designer(managers):
        orders = {}
        for m in managers:
            designers = connect_and_query(qry_get_roles_in_manager_branch, [m['UserIndex'], 13] )  # 13=designer
            if designers is None or len(designers) == 0:
                continue
            for designer in designers:
                designer_orders = connect_and_query(
                    qry = qry_get_commited_task_alef_from_previous_day,
                    qry_params = [None, designer['EmployeeUserIndex']]
                    )
                if designer_orders is None or len(designer_orders) == 0:
                    continue
                orders[designer['EmployeeMail']] = designer_orders
        return orders 
    
    @task()
    def send_mails(orders):
        for mail_adress in orders.keys():
            send_mail_if_not_empty(to=[mail_adress], cc = ['snir-y@regba.co.il', 'lior-r@regba.co.il'] , qry_result = orders[mail_adress])
            sleep(2)
        return None
    
    Managers = get_managers()
    manager_orders = get_orders_by_manager(Managers)
    send_mails(manager_orders)
    designer_orders = get_orders_by_designer(Managers)
    send_mails(designer_orders)

main_dag = commited_tasks_alef()
