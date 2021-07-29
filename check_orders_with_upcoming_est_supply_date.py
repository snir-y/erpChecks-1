from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import os
import requests
import json
from erpChecks.queries import qry_get_managers, qry_get_orders_with_approaching_supplydate_and_no_requested_seria
from erpChecks.mail_body import mail_body_upcoming_supplydate


default_args = {
    'owner': 'Snir',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

faas_url = 'http://' + os.environ['LOCAL_SRV_IP'] +':8080'   # prod
# faas_url = http://172.16.11.89:8080 #test
d_con = {'DB_NAME': 'WiseERPRegba',
         'DB_SRV': '172.16.11.5'}

def connect_and_query(userindex):
    query_data = {'d_con': d_con,
                'qry': qry_get_orders_with_approaching_supplydate_and_no_requested_seria,
                'qry_params': [userindex],
                'lower_keys': False}
    r = requests.post('{}/function/mssql'.format(faas_url),  # prod
                    data=json.dumps(query_data))
    if r.status_code != 200:
        return None
    qry_result = r.json()
    if qry_result is None or 'data_result' not in qry_result.keys():
        return None
    return qry_result['data_result']

def send_mail_if_not_empty(mailadress, qry_result):
    if qry_result is None or len(qry_result) == 0:
        return None
    json2tbl_url = '{}/function/json2tbl'.format(faas_url)  # prod
    req = {'table': qry_result}
    r_table = requests.post(json2tbl_url, data=json.dumps(req))
    # refactor this
    mail_url = '{}/function/mail'.format(faas_url)  # prod
    mail_data = {
        'recipient': ['snir-y@regba.co.il'],
        # 'cc': ['snir-y@regba.co.il'], #  'lior-r@regba.co.il'
        'subject': 'הזמנות שמתקרבות לתאריך האספקה המשוער שלהן - הודעה אוטומטית ',
        # notice mail_body_old_supplydate is global
        'content': mail_body_upcoming_supplydate + r_table.content.decode('utf-8'),
        'attachments': [],
        'footer': 'DAG: ERPChecks -  check_orders_with_upcoming_est_supplydate'
    }
    mail_data['title'] = mail_data['subject']

    r_mail = requests.post(mail_url, data=json.dumps(mail_data))
    return r_mail.status_code



@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(0))
def check_orders_with_upcoming_est_supply_date():
    @task()
    def get_managers():
        query_data = {'d_con': d_con,
                    'qry': qry_get_managers}
        r = requests.post('{}/function/mssql'.format(faas_url),
                        data=json.dumps(query_data))
        qry_result = r.json()
        return qry_result['data_result']
    
    @task()
    def get_orders(managers):
        orders = {}
        for m in managers:
            data = connect_and_query(m['UserIndex'])
            if data is not None:
                orders[m['EmployeeEmail']] =  data      
        return orders
    
    @task()
    def send_mails(orders_by_manager):
        for mail_adress in orders_by_manager.keys():
            send_mail_if_not_empty('snir-y@regba.co.il', orders_by_manager[mail_adress])
        return None


    Managers = get_managers()
    orders = get_orders(Managers)
    send_mails(orders)

main_dag = check_orders_with_upcoming_est_supply_date()




# dag = DAG('check_orders_with_upcoming_est_supplydate',
#           description='checks the estimated supply date',
#           default_args=default_args,
#           schedule_interval='15 9 * * 0',
#           catchup=False)

# with dag:
#     start = DummyOperator(task_id='start')
#     for manager in default_args['Managers']:

#         mail_and_connets_N_query_operator = PythonOperator(task_id='send_mail_if_not_empty_{0}'.format(manager[0]),
#                                        python_callable=send_mail_if_not_empty, dag=dag, op_args=[manager[0], manager[1]], provide_context=False)

#         start >> mail_and_connets_N_query_operator
