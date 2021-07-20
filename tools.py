import requests
import json


def connect_and_query(qry,  params, d_con, faas_url):
    query_data = {'d_con': d_con,
                  'qry': qry,
                  'qry_params': params if params is not None and len(params) > 0 else None,
                  'lower_keys': False}
    r = requests.post('{}/function/mssql'.format(faas_url),  # prod
                      data=json.dumps(query_data))
    if r and r.status_code == 200:
        qry_result = r.json()
        return qry_result['data_result']
    else:
        return None


def send_mail_if_not_empty(mailadress_to, mailadress_cc, subject, body, qry_result, footer, faas_url):
    if qry_result is None or len(qry_result) == 0:
        return None

    json2tbl_url = '{}/function/json2tbl'.format(faas_url)  # prod
    req = {'table': qry_result}
    r_table = requests.post(json2tbl_url, data=json.dumps(req))

    mail_url = '{}/function/mail'.format(faas_url)  # prod
    mail_data = {
        'recipient': mailadress_to,  # list
        'cc': mailadress_cc,  # list
        'subject': subject,
        'content':  body + r_table.content.decode('utf-8'),
        'attachments': [],
        'footer':  footer
    }
    mail_data['title'] = mail_data['subject']

    r_mail = requests.post(mail_url, data=json.dumps(mail_data))
    return r_mail.status_code
