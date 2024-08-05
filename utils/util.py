
import boto3
from botocore.exceptions import ClientError
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# Get secret from AWS Secret Manager
def get_secret(user='notnotsez'):

    secret_name = f'snowflake-user-{user}'

    region_name = "ap-southeast-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response['SecretString'])

    return secret



# Format long numbers with comma notation
def format_number_with_commas(number):
    return "{:,}".format(number)



# Utility function to create a Snowflake connection
def get_snowflake_connection(secret, schema):
    return snowflake.connector.connect(
        user=secret['user'],
        password=secret['password'],
        account=secret['account'],
        warehouse=secret['warehouse'],
        database='ARBITRUM_NOVA',
        schema=schema,
        login_timeout=30  # Increase the timeout for login
    )



# Fetch the latest timestamp and block number from the Snowflake table
def fetch_latest_block_number(secret, schema, table_name):
    conn = get_snowflake_connection(secret, schema)
    query = f"SELECT MAX(block_number) AS max_block FROM {table_name}"
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0]



# Function to perform delta append using pandas to_sql
def delta_append(secret, table_name, df):
    conn = get_snowflake_connection(secret, 'STAGING')
    cursor = conn.cursor()

    # Append new data using write_pandas
    if not df.empty:
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
        if success:
            print(f"Inserted {nrows} rows into {table_name}")
        else:
            print(f"Failed to insert data into {table_name}")

    cursor.close()
    conn.close()