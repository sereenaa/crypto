
import os
import cryo
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the Ethereum RPC URL from environment variables
rpc = os.getenv("ETH_RPC")

# Collect blockchain data using the cryo library and return it as a pandas DataFrame
# Specifying blocks range and output format
data = cryo.collect(
    "blocks", 
    blocks=["28382322:28382323"], # includes the first block but not the last 
    rpc=rpc, 
    output_format="pandas", 
    hex=True
)

data = cryo.collect(
    "txs", 
    blocks=["28382322:28382323"], # includes the first block but not the last 
    rpc=rpc, 
    output_format="pandas", 
    hex=True
)
