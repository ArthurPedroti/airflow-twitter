import sys
sys.path.append("airflow")

from hook.protheus_api_hook import ProtheusApiHook
from airflow.models import BaseOperator, DAG, TaskInstance
import json
import psycopg2
from datetime import datetime

class AverageOperator(BaseOperator):
    
    template_fields = ["route", "request_params"]

    def __init__(self, route, request_params, **kwargs):
        self.route = route
        self.request_params = request_params
        super().__init__(**kwargs)
        
    def save_data_to_postgres(self, data):
        try:
            # Replace these with your PostgreSQL database credentials
            conn = psycopg2.connect(
                database='agf_datawarehouse',
                user='postgres',
                password='postgres',
                host='localhost',
                port='5432'
            )

            cursor = conn.cursor()

            # Create a table if it doesn't exist
            create_table_query = """
                CREATE TABLE IF NOT EXISTS average (
                    CODIGO TEXT,
                    Q01 INTEGER,
                    Q02 INTEGER,
                    Q03 INTEGER,
                    Q04 INTEGER,
                    Q05 INTEGER,
                    Q06 INTEGER,
                    Q07 INTEGER,
                    Q08 INTEGER,
                    Q09 INTEGER,
                    Q10 INTEGER,
                    Q11 INTEGER,
                    Q12 INTEGER
                );
            """
            cursor.execute(create_table_query)

            # Insert data into the table
            insert_query = """
                INSERT INTO average (CODIGO, Q01, Q02, Q03, Q04, Q05, Q06, Q07, Q08, Q09, Q10, Q11, Q12)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            values = (
                data['CODIGO'], data['Q01'], data['Q02'], data['Q03'], data['Q04'],
                data['Q05'], data['Q06'], data['Q07'], data['Q08'], data['Q09'],
                data['Q10'], data['Q11'], data['Q12']
            )
            cursor.execute(insert_query, values)

            conn.commit()
            print("Data saved successfully to PostgreSQL.")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL or saving data:", error)
        finally:
            # Close the database connection
            if conn:
                cursor.close()
                conn.close()
    
    def execute(self, context):
        route = self.route
        request_params = self.request_params
        print(route, request_params)
        
        api_data = ProtheusApiHook(route, request_params).run()
        if api_data:
            # print(api_data)
            self.save_data_to_postgres(api_data[0])


if __name__ == "__main__":
    route = "average"
    request_params = {
        "filial": "0101",
        "produto": "VIXMOT0011"
    }
    
    with DAG(dag_id = "ProtheusApiTest", start_date=datetime.now()) as dag:
        to = AverageOperator(
          route=route, 
          request_params=request_params,
          task_id="test_run"
        )
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)