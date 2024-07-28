import os
from util import *
from get_secret import get_secret
secret = get_secret(user='notnotsez')
rpc = os.getenv("ETH_RPC")

table = 'transactions'

conn = get_snowflake_connection(secret)
query = f"""
WITH distinct_blocks AS (
    SELECT DISTINCT block_number
    FROM proofofplay.raw.{table}
),
block_range AS (
    SELECT MIN(block_number) AS min_block, MAX(block_number) AS max_block
    FROM distinct_blocks
),
recursive_blocks AS (
    SELECT min_block AS block_number
    FROM block_range
    UNION ALL
    SELECT block_number + 1
    FROM recursive_blocks, block_range
    WHERE block_number < max_block
)
SELECT block_number AS missing_block_number
FROM recursive_blocks
LEFT JOIN distinct_blocks USING (block_number)
WHERE distinct_blocks.block_number IS NULL
ORDER BY missing_block_number;
"""
cursor = conn.cursor()
cursor.execute(query)
result = cursor.fetchall()
cursor.close()
conn.close()

for block_num in result:
    print(block_num[0])
    start_block = block_num[0]
    end_block = start_block + 1 
    if table == 'transactions': 
        fetch_and_push_transactions_with_cryo(secret, rpc, start_block, end_block)
    elif table == 'logs':
        fetch_and_push_logs_with_cryo(secret, rpc, start_block, end_block)