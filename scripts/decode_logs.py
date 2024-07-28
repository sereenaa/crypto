

import pandas as pd
from web3 import Web3
from util import * 

# Connect to a Web3 provider
w3 = Web3(Web3.HTTPProvider('https://rpc.apex.proofofplay.com'))
secret = get_secret(user='notnotsez')

conn = get_snowflake_connection(secret)
query = f"""
select * from proofofplay.raw.logs 
where block_number = 28383712 
    and transaction_hash = '0x4d86329ff5651579352c8ee99a79873f73ca57bf8dff1c82299f6dcfa6b41495' 
    and log_index = 11
;
"""
cursor = conn.cursor()
cursor.execute(query)
df = cursor.fetch_pandas_all()
cursor.close()
conn.close()

# Decode all logs
decoded_logs = [decode_log(log) for log in df.to_dict(orient='records')]

# Print decoded logs
for decoded_log in decoded_logs:
    print(decoded_log)


decoded_logs_df = pd.DataFrame(data=decoded_logs)
decoded_logs_df.to_csv('decoded_logs_0x7a4a4a586295c9b87b8a42f3b3b5709c475715d2feebcf9415dd3c8c668cc545.csv')