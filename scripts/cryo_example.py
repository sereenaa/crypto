
import os
import cryo
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

# Retrieve the Ethereum RPC URL from environment variables
rpc_url = os.getenv("RPC_URL")

# Collect blockchain data using the cryo library and return it as a pandas DataFrame
# Specifying blocks range and output format
opcodes_data = cryo.collect(
    "geth_opcodes", 
    blocks=["53155869:53155879"], 
    rpc=rpc_url, 
    output_format="pandas", 
    hex=True,
    requests_per_second=100,
    max_retries=10, 
    initial_backoff=1000,
    include_columns=['block_number']
)


df = opcodes_data[opcodes_data['block_number']==53155869]


# Collect blockchain data using the cryo library and return it as a pandas DataFrame
# Specifying blocks range and output format
storage_data = cryo.collect(
    "storage_reads", 
    blocks=["53155869:53155870"], 
    rpc=rpc_url, 
    output_format="pandas", 
    hex=True,
    requests_per_second=100,
    max_retries=10, 
    initial_backoff=1000,
    include_columns=['block_number']
)



# Collect blockchain data using the cryo library and return it as a pandas DataFrame
# Specifying blocks range and output format
start_time = time.time()
geth_storage_diffs_data = cryo.collect(
    "geth_storage_diffs", 
    blocks=["53155869:53156869"], 
    rpc=rpc_url, 
    output_format="pandas", 
    hex=True,
    requests_per_second=100,
    max_retries=10, 
    initial_backoff=1000,
    include_columns=['block_number']
)
end_time = time.time()
print(f'Time taken: {end_time-start_time:.2f} seconds')

geth_storage_diffs_data.to_csv('data/geth_storage_diffs_data.csv')

