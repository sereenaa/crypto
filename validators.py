import asyncio
import csv
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import json
import requests
from get_secret import get_secret

async def fetch_data_and_save_as_csv():
    transport = AIOHTTPTransport(url="https://cache-service.chainflip.io/graphql")

    async with Client(
        transport=transport,
        fetch_schema_from_transport=True,
    ) as session:
        query = gql(
          """
          fragment CacheValidator on Validator {
            idHex
            idSs58
            alias
            apyBp
            boundRedeemAddress
            processorId
            totalRewards
            isCurrentAuthority
            isCurrentBackup
            isQualified
            isOnline
            isBidding
            isKeyholder
            reputationPoints
            lockedBalance
            unlockedBalance
            firstFundingTimestamp
            latestFundingTimestamp
            __typename
          }

          query Validators {
            validators: allValidators {
              nodes {
                ...CacheValidator
                __typename
              }
              __typename
            }
          }
          """
        )

        result = await session.execute(query)
        print(result)

        # Assuming the result is a list of validators
        validators = result['validators']['nodes']

        # Write to CSV
        keys = validators[0].keys()  # Get field names from the first validator
        csv_content = []
        for validator in validators:
          csv_content.append(validator)

        return csv_content, keys


dune_api_key = get_secret()

def upload_csv(csv_content, keys):
  api_key = dune_api_key
  csv_file_path = '/tmp/validators.csv'

  # Write to CSV file in temporary storage
  with open(csv_file_path, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=keys)
    writer.writeheader()
    writer.writerows(csv_content)

  url = 'https://api.dune.com/api/v1/table/upload/csv'

  with open(csv_file_path) as open_file:
    data = open_file.read()

    headers = {'X-Dune-Api-Key': api_key}
    payload = {
      "table_name": "chainflip_validators",
      "description": "validator_data_for_chainflip",
      "is_private": False,
      "data": str(data)
    }

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print('Response status code:', response.status_code)
    print('Response content:', response.content)



def validators_main():
  loop = asyncio.get_event_loop()
  csv_content, keys = loop.run_until_complete(fetch_data_and_save_as_csv())
  upload_csv(csv_content, keys)
