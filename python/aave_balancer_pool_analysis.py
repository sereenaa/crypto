from eth_abi import decode
import pandas as pd

# Set display options to show the full DataFrame
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Auto-detect the width of the display
pd.set_option('display.max_colwidth', None)  # Show full content of each column


df = pd.read_csv('csvs/20wstETH-80AAVE-2024-10-06.csv')

# Define the ABI types for decoding
abi_types = [
    'address[]',  # tokens
    'int256[]',   # deltas
    'uint256[]'   # protocolFeeAmounts
]

# Function to decode a single encoded data entry
def decode_data(encoded_data):
    # Convert hex string to bytes and decode
    decoded = decode(abi_types, bytes.fromhex(encoded_data[2:]), strict=False)
    return decoded

# Apply the decode function to the 8th column
decoded_data = df.iloc[:, 7].apply(decode_data)

# Expand the results into new columns
df['token_1'] = decoded_data.apply(lambda x: x[0][0])
df['token_2'] = decoded_data.apply(lambda x: x[0][1])
df['delta_1'] = decoded_data.apply(lambda x: x[1][0])
df['delta_2'] = decoded_data.apply(lambda x: x[1][1])
df['protocolFeeAmount_1'] = decoded_data.apply(lambda x: x[2][0])
df['protocolFeeAmount_2'] = decoded_data.apply(lambda x: x[2][1])

# Display the updated DataFrame
print(df)