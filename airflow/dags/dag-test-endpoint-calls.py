from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def print_titulo():
    print('Teste consulta APIs Mock e externa (ViaCEP)')

def print_date():
    print('Momento atual {}'.format(datetime.today().strftime('%d/%m/%Y %H:%M:%S')))

def chamar_api_viacep():
    response = requests.get('https://viacep.com.br/ws/01001000/json/')
    informacoes = response.json()
    print('\nResultado consulta API ViaCEP: "{}"\n'.format(informacoes))

def chamar_api_mock_customers():
    response = requests.get('http://mock-server:3000/api/customers')
    informacoes = response.json()
    print('\nResultado consulta API Mock Customers: "{}"\n'.format(informacoes))

dag = DAG(
    'dag-test-endpoint-calls',
    default_args={'start_date': datetime.now() - timedelta(days=1)},
    schedule=timedelta(minutes=1),#'45 17 * * *', # aos 45 minutos de das 17 horas todo dia, mês e semana
    catchup=False
)

print_mensagem_inicial_task = PythonOperator(
    task_id='print_mensagem',
    python_callable=print_titulo,
    dag=dag
)

chamar_api_mock_customers_task = PythonOperator(
    task_id='chamar_api_mock_customers',
    python_callable=chamar_api_mock_customers,
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)

chamar_api_viacep_task = PythonOperator(
    task_id='chamar_api_viacep',
    python_callable=chamar_api_viacep,
    dag=dag
)

print_mensagem_inicial_task >> chamar_api_mock_customers_task >> print_date_task >> chamar_api_viacep_task