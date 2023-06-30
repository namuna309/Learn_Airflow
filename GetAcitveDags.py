import requests
from requests.auth import HTTPBasicAuth

def get_active_dags(api_url, username, password):
    response = requests.get(api_url + '/dags', auth=HTTPBasicAuth(username, password))
    response.raise_for_status()
    dags = response.json()['dags']
    active_dags = []

    for dag in dags:
        dag_id = dag['dag_id']
        is_active = dag['is_active']
        is_paused = dag['is_paused']
        if not is_paused and is_active:
            active_dags.append(dag_id)

    return active_dags

# Airflow API URL, 사용자 이름 및 비밀번호
api_url = 'http://localhost:8080/api/v1'
username = 'airflow'
password = 'airflow'

active_dags = get_active_dags(api_url, username, password)
print(active_dags)