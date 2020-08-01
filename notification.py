import json
import time
import emails
import requests

EMAIL_TO_ADDRESS = 'akhil23prasad@gmail.com'
EMAIL_TO_NAME = 'Akhil Prasad'
EMAIL_FROM_ADDRESS = 'griffin_dq_agent@testmail.com'
EMAIL_FROM_NAME = 'Griffin DQ checker'

SMTP_SERVER_IP = '127.0.0.1'
# SMTP_SERVER_IP = '52.14.220.226'
SMTP_SERVER_PORT = '25'
SMTP_SERVER_TIMEOUT = 5

REST_API_IP = '127.0.0.1'
# REST_API_IP = '52.14.220.226'
REST_API_PORT = '38080'
REST_API_URL = 'http://' + REST_API_IP + ':' + REST_API_PORT

json_string = open('griffin.json', 'r').read()
json_string = json_string.replace('{{BASE_PATH}}', REST_API_URL)
griffin_api_json = json.loads(json_string)

loaded_jobs_timestamp = []

threshold = {
    'ACCURACY': ['percentage < 90'],
    'PROFILING': ['email_nullcount >= 2', 'name_emptycount >= 2', 'age_min <= 0', 'age_max >= 150']    
}

def send_email(subject, html_body):
    message = emails.html(
        html=html_body,
        subject=subject,
        mail_from=(EMAIL_FROM_NAME, EMAIL_FROM_ADDRESS)
    )
    r = message.send(
        to=(EMAIL_TO_NAME, EMAIL_TO_ADDRESS),
        render={'name': EMAIL_TO_NAME},
        smtp={'host': SMTP_SERVER_IP, 'port': SMTP_SERVER_PORT, 'timeout': SMTP_SERVER_TIMEOUT}
    )
    return r

def item_generator(json_input, lookup_key, lookup_value):
    lookup_value = lookup_value.lower()
    if isinstance(json_input, dict):
        for k, v in json_input.items():
            if k == lookup_key and type(v) == str and v.lower() == lookup_value:
                yield json_input
            else:
                yield from item_generator(v, lookup_key, lookup_value)
    elif isinstance(json_input, list):
        for item in json_input:
            yield from item_generator(item, lookup_key, lookup_value)

def search_item_by_name(field_name, field_value):
    item = [x for x in item_generator(griffin_api_json, field_name, field_value)]
    return item[0] if len(item) > 0 else None

def get_request_url(item):
    return item['request']['url']['raw']

def get_metrics():
    field_value = "Get metrics"
    field_name = "name"
    item = search_item_by_name(field_name, field_value)
    metrics = requests.get(get_request_url(item))
    return metrics

def check_for_notification_threshold():
    metrics = get_metrics()
    metrics = metrics.json()
    filed_values = ['ACCURACY', 'PROFILING', 'UNIQUENESS']
    for field_value in filed_values:
        filter_metrics = [x for x in item_generator(metrics, 'type', field_value)]
        try:
            for metric in filter_metrics:
                metricValues = metric['metricValues']
                maxtimestamp = 0
                for mv in metricValues:
                    timestamp = int(mv['tmst'])
                    if timestamp > maxtimestamp:
                        maxtimestamp = timestamp
                        if (metric['name'], timestamp) not in loaded_jobs_timestamp:
                            print("Duplicate:", (metric['name'], timestamp))
                            latest_job_instance.append((field_value, mv))
                        loaded_jobs_timestamp.append((metric['name'], timestamp))
        except Exception as e:
            print(e)

def triger_notification_accuracy(instance):
    job_name = instance['name']
    total = instance['value']['total']
    matched = instance['value']['matched']
    miss = instance['value']['miss']
    time_t = time.ctime( int(instance['tmst'])/1000 )
    # tmst_condition = int(instance['tmst'])/1000 >= time.time() - 86400 - 120
    percentage = matched / total * 100
    # print(int(instance['tmst'])/1000, time.time()-86400-120, int(instance['tmst'])/1000 >= time.time()-86400-120)
    threshold_triggered = []
    if True:
        for cond in threshold['ACCURACY']:
            try:
                if eval(cond):
                    threshold_triggered.append(cond)
            except:
                pass
        if len(threshold_triggered):
            threshold_triggered_s = ', '.join(threshold_triggered)
            email_subject = f"Accuracy check status: Job - {job_name}"
            email_body = f"""
                Job: {job_name}<br />
                Time: {time_t}<br />
                Thresholds triggered: {threshold_triggered_s}%<br />
                <br />
                Accuracy: {percentage:.2f}%<br />
                Total Records: {total}<br />
                Total Match: {matched}<br />
                Total Missmatch: {miss}<br />
            """
            print(email_subject)
            print(email_body)
            r = send_email(email_subject, email_body)
            print("Sending Mail")
            print(r)
    else:
        print("Timestamp is old Email will not be triggered:",job_name,instance['tmst'])

def triger_notification_profiling(instance):
    job_name = instance['name']
    try:
        email_nullcount = instance['value']['email_nullcount']
    except Exception as e:
        print(e)
        email_nullcount = None
    try:
        name_emptycount = instance['value']['name_emptycount']
    except Exception as e:
        print(e)
        name_emptycount = None
    try:
        age_min = instance['value']['age_min']
    except Exception as e:
        print(e)
        age_min = None
    try:
        age_max = instance['value']['age_max']
    except Exception as e:
        print(e)
        age_max = None
    time_t = time.ctime( int(instance['tmst'])/1000 )
    print(email_nullcount, name_emptycount, age_min, age_max)
    # tmst_condition = int(instance['tmst'])/1000 >= time.time() - 86400 - 120
    threshold_triggered = []
    if True:
        for cond in threshold['PROFILING']:
            try:
                if eval(cond):
                    threshold_triggered.append(cond)
                print(cond, eval(cond))
            except:
                pass
        if len(threshold_triggered):
            threshold_triggered_s = ', '.join(threshold_triggered)
            email_subject = f"Profiling check status: Job - {job_name}"
            email_body = f"""
                Job: {job_name}<br />
                Time: {time_t}<br />
                Thresholds triggered: {threshold_triggered_s}<br />
                <br />
                Null email count: {email_nullcount}<br />
                Empty name count: {name_emptycount}<br />
                MIN Age in dataset: {age_min}<br />
                MAX Age in dataset: {age_max}<br />
            """
            print(email_subject)
            print(email_body)
            r = send_email(email_subject, email_body)
            print("Sending Mail")
            print(r)
    else:
        print("Timestamp is old Email will not be triggered:", job_name, instance['tmst'])

latest_job_instance = []
check_for_notification_threshold()
while(True):
    latest_job_instance = []
    check_for_notification_threshold()
    for instance in latest_job_instance:
        if instance[0] == 'ACCURACY':
            triger_notification_accuracy(instance[1])
        elif instance[0] == 'PROFILING':
            triger_notification_profiling(instance[1])
    time.sleep(10)
