import requests
import time
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

# Replace with your Dune API Key
DUNE_API_KEY = os.getenv('DUNE_API_KEY')

# The query ID from your Dune query
QUERY_ID = "4084682"

# Headers for API requests
headers = {
    "x-dune-api-key": DUNE_API_KEY,
    "Content-Type": "application/json"
}

# Step 1: Execute the query
def execute_query(query_id):
    execution_url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
    response = requests.post(execution_url, headers=headers)
    response_data = response.json()

    # Check if the query execution was successful
    if response.status_code != 200:
        raise Exception(f"Failed to execute query: {response_data}")

    execution_id = response_data['execution_id']
    print(f"Query execution started. Execution ID: {execution_id}")
    return execution_id

# Step 2: Poll the execution status
def poll_execution_status(execution_id):
    status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
    
    while True:
        response = requests.get(status_url, headers=headers)
        result = response.json()

        state = result.get("state")
        if state == "QUERY_STATE_COMPLETED":
            print("Query completed!")
            return
        elif state == "QUERY_STATE_FAILED":
            raise Exception("Query failed!")
        else:
            print(f"Query is still running. Current state: {state}")
            time.sleep(5)  # Wait for 5 seconds before polling again

# Step 3: Get the query results in CSV
def get_query_results_csv(execution_id):
    csv_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results/csv"
    
    response = requests.get(csv_url, headers=headers)
    
    # Save the CSV file locally
    with open("dune_query_results.csv", "wb") as file:
        file.write(response.content)
    print("Results saved as dune_query_results.csv")

# Main function to execute the steps
def main():
    try:
        # Step 1: Execute the query and get the execution_id
        execution_id = execute_query(QUERY_ID)
        
        # Step 2: Poll the execution status until completion
        poll_execution_status(execution_id)
        
        # Step 3: Download the CSV result
        get_query_results_csv(execution_id)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()