
import cryo
import os

# Set the RPC URL
os.environ["ETH_RPC_URL"] = "https://rpc.apex.proofofplay.com"

# Fetch transaction data
# 28382322:28382323
tx_data = cryo.transactions(blocks="28382322-28382323")
