import json
import requests

def upload_csv_to_dune(dune_api_key, csv_file_path, table_name, description): 

  url = 'https://api.dune.com/api/v1/table/upload/csv'

  with open(csv_file_path) as open_file:
    data = open_file.read()

    headers = {'X-Dune-Api-Key': dune_api_key}
    payload = {
      "table_name": f"{table_name}",
      "description": f"{description}",
      "is_private": False,
      "data": str(data)
    }

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print('Response status code:', response.status_code)
    print('Response content:', response.content)
