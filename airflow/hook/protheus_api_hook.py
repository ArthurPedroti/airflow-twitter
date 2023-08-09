from datetime import datetime, timedelta
from airflow.providers.http.hooks.http import HttpHook
import requests
import json

class ProtheusApiHook(HttpHook):

    def __init__(self, route, params = {}, conn_id=None,):
        self.route = route
        self.params = params
        self.conn_id = conn_id or "protheus_api"
        super().__init__(http_conn_id=self.conn_id)
      
    def create_url(self):
        route = self.route

        url_raw = f"{self.base_url}/{route}"

        return url_raw
  
    def connect_to_endpoint(self, url, params, session):
        request = requests.Request("GET", url, params=params)  
        prep = session.prepare_request(request)
        self.log.info(f"URL: {request.url}")
        self.log.info(f"Params: {request.params}")
        
        return self.run_and_check(session, prep, {})
      
    def request(self, url_raw, session):
        params = self.params
        response = self.connect_to_endpoint(url_raw, params, session)
        
        json_response = response.json()

        return json_response
      
    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        
        return self.request(url_raw, session)
      
if __name__ == "__main__":
    route = "average"
    params = {
        "filial": "0101",
        "produto": "VIXMOT0011"
    }
    
    response = ProtheusApiHook(route, params).run()
    print(json.dumps(response, indent=4, sort_keys=True))
    