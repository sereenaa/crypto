import csv
import json
import requests
from get_secret import get_secret
from util import upload_csv_to_dune

def fetch_data(url):
	try:
		response = requests.get(url)
		if response.status_code == 200:
			return response.json()  # Returns the JSON data from the response
		else:
			return f"Error: Unable to fetch data, status code {response.status_code}"
	except Exception as e:
		return f"Error: {str(e)}"


def circulation_main(): 
	url = "https://cache-service.chainflip.io/circulation"

	data = fetch_data(url)

	with open('/tmp/circulation.csv', 'w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(data.keys())
		writer.writerow(data.values())

	dune_api_key = get_secret()

	upload_csv_to_dune(dune_api_key=dune_api_key, csv_file_path='/tmp/circulation.csv', table_name='chainflip_circulation', description='circulation_data_for_chainflip')


	
