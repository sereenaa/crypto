import json
from circulation import circulation_main
from gmx_positions import gmx_positions_main
from validators import validators_main

def lambda_handler(event, context):

  try: 
    validators_main()
    circulation_main()
    gmx_positions_main()

    return {
      'statusCode': 200,
      'body': json.dumps('Lambda function executed successfully!')
    }

  except Exception as e:
    return {
      'statusCode': 400,
      'body': json.dumps(f'Lambda function failed. Error: {e}')
    }