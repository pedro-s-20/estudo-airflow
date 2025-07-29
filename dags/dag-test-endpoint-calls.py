from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sdk import Variable
import requests

def chamar_api_viacep():
    response = requests.get('https://viacep.com.br/ws/01001000/json/')
    informacoes = response.json()
    print('\nResultado consulta API ViaCEP: "{}"\n'.format(informacoes))

def chamar_api_mock_customers():
    token = Variable.get('fake_token')
    headers = {
        'authorization': f"Bearer {token}"
    }
    response = requests.get('http://mock-server:3000/api/customers', headers=headers)
    informacoes = response.json()
    print('\nResultado consulta API Mock Customers: "{}"\n'.format(informacoes))

def chamar_api_receitaws():
    response = requests.get('https://receitaws.com.br/v1/cnpj/51268701000147')
    informacoes = response.json()
    print('\nResultado consulta API ReceitaWS: "{}"\n'.format(informacoes))

dag = DAG(
    'dag-test-endpoint-calls',
    default_args={'start_date': datetime.now() - timedelta(days=1)},
    schedule=timedelta(minutes=2),
    catchup=False
)

chamar_api_mock_customers_task = PythonOperator(
    task_id='chamar_api_mock_customers',
    python_callable=chamar_api_mock_customers,
    dag=dag
)

chamar_api_viacep_task = PythonOperator(
    task_id='chamar_api_viacep',
    python_callable=chamar_api_viacep,
    dag=dag
)

chamar_api_receitaws_task = PythonOperator(
    task_id='chamar_api_receitaws',
    python_callable=chamar_api_receitaws,
    dag=dag
)

chamar_api_mock_customers_task >> chamar_api_viacep_task >> chamar_api_receitaws_task