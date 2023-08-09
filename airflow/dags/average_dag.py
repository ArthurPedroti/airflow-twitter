import sys
sys.path.append("airflow")

from airflow.models import DAG
from airflow.utils.dates import days_ago
from operators.average_operator import AverageOperator

with DAG(dag_id = "AverageDag", start_date=days_ago(6), schedule_interval="@daily") as dag:
    route = "average"
    request_params = {
        "filial": "0101",
        "produto": "VIXMOT0011"
    }

    to = AverageOperator(
          route=route, 
          request_params=request_params,
          task_id="test_run"
        )