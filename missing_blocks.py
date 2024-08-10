
from dotenv import load_dotenv
import os 
import pandas as pd 

from utils.util import *
from utils.process_data import *

# Load environment variables from .env file
load_dotenv()

# Get config
secret = get_secret(user='notnotsez-peter')
table_name = os.getenv("TABLE_NAME")
rpc_url = os.getenv("RPC_URL_1")

missing_blocks_df = pd.read_csv('data/missing_blocks.csv')
missing_blocks_list = missing_blocks_df.iloc[:, 0].tolist()


for block in missing_blocks_list:
    fetch_and_push_raw_opcodes_for_block_range(secret, 'OPCODES', rpc_url, block, block+1)