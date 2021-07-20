import tools
import queries
from datetime import datetime, timedelta


default_args = {
    'owner': 'Snir',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

default_args['d_con'] = {'DB_NAME': 'WiseERPRegba',
                         'DB_SRV': '172.16.11.5'}
default_args['faas_url'] = 'http://'+os.environ['LOCAL_SRV_IP']+':8080'   # prod

Managers = tools.connect_and_query(userindex=1, qry=queries.qry_get_managers,
                                   d_con=default_args['d_con'], faas_url=default_args['faas_url'])
Managers = [(x['UserIndex'], x['EmployeeEmail']) for x in Managers]
