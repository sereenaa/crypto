
select * 
from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers`
where timestamp_trunc(block_timestamp, hour) = timestamp('2024-10-06 03:00:00')
  and address in (
    '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'
    , '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'
  )

select * 
    from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers`
    where timestamp_trunc(block_timestamp, hour) between timestamp('2024-10-01 00:00:00') and timestamp('2024-10-06 00:00:00')
		and address in ('0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0')


select * from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.decoded_events`
  where timestamp_trunc(block_timestamp, day) = timestamp('2024-10-06')
  and transaction_hash = '0x067e7e62767d517b9e04ac1ef86a9ab47066014e2ab378c545b10bfcb7d35990'
  order by log_index


select * from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.logs`
  where timestamp_trunc(block_timestamp, day) = timestamp('2024-10-06')
  and transaction_hash = '0x067e7e62767d517b9e04ac1ef86a9ab47066014e2ab378c545b10bfcb7d35990'
  order by log_index


select * from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.logs` 
where timestamp_trunc(block_timestamp, day) between timestamp('2024-10-06') and timestamp('2024-10-08')
  and array_length(topics) > 2
  and topics[0] = '0xe5ce249087ce04f05a957192435400fd97868dba0e6a4b4c049abf8af80dae78'
  and topics[1] = '0x3de27efa2f1aa663ae5d458857e731c129069f29000200000000000000000588'
order by block_timestamp