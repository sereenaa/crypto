
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
