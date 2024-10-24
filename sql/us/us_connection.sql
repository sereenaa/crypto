
select * 
from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.logs` 
where timestamp_trunc(block_timestamp, hour) = timestamp_trunc(timestamp('2024-10-17 06:00:00'), hour)
and array_length(topics) > 1
  and topics[0] = '0xe5ce249087ce04f05a957192435400fd97868dba0e6a4b4c049abf8af80dae78'
  and topics[1] = '0x3de27efa2f1aa663ae5d458857e731c129069f29000200000000000000000588'
;


select * 
from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.logs` 
where timestamp_trunc(block_timestamp, hour) between timestamp_trunc(timestamp('2024-10-14 00:00:00'), hour) and timestamp_trunc(timestamp('2024-10-17 08:00:00'), hour)
and array_length(topics) > 1
  and topics[0] = '0xe5ce249087ce04f05a957192435400fd97868dba0e6a4b4c049abf8af80dae78'
  and topics[1] = '0x3de27efa2f1aa663ae5d458857e731c129069f29000200000000000000000588'
;


select * 
    from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers`
    where timestamp_trunc(block_timestamp, hour) between timestamp('2024-09-01 00:00:00') and timestamp('2024-09-02 00:00:00')
		and address in ('0x6c3ea9036406852006290770bedfcaba0e23a0e8') -- PYUSD
;


select * 
    from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers`
    where timestamp_trunc(block_timestamp, hour) between timestamp('2024-09-01 00:00:00') and timestamp('2024-09-02 00:00:00')
		and address in ('0x0c0d01abf3e6adfca0989ebba9d6e85dd58eab1e') -- aEthPYUSD
;