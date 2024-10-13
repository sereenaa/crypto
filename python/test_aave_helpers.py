from datetime import datetime 
from decimal import Decimal 
import json
from web3 import Web3
from multicall import Call, Multicall
from tl_pipelines.resources.aave_helpers import get_raw_reserve_data
from tl_pipelines.resources.aave_config import CONFIG_AAVE_MARKETS
from tl_pipelines.resources.common_config import CONFIG_CHAINS

# Initialize Web3
chain = "ethereum"
market = "ethereum_v3"
w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS['ethereum']['web3_rpc_url']))

# Example asset and block height
reserve = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # Example reserve aEthWETH
# reserve = "0xba100000625a3754423978a60c9317c58a424e3D" # Example reserve aBAL
block_height = 20954000  # Example block height
decimals = 18  # Example decimals

# Pool contract address
pool_address = CONFIG_AAVE_MARKETS[market]['pool']
data_provider = CONFIG_AAVE_MARKETS[market]['protocol_data_provider']



def v3_reserve_data_handler(value):
    return {
        'unbacked_atokens': Decimal(value[0]) / Decimal(10 ** decimals),
        'scaled_accrued_to_treasury': Decimal(value[1]) / Decimal(10 ** decimals),
        'atoken_supply': Decimal(value[2]) / Decimal(10 ** decimals),
        'stable_debt': Decimal(value[3]) / Decimal(10 ** decimals),
        'variable_debt': Decimal(value[4]) / Decimal(10 ** decimals),
        'liquidity_rate': Decimal(value[5]) / Decimal(10 ** 27),
        'variable_borrow_rate': Decimal(value[6]) / Decimal(10 ** 27),
        'stable_borrow_rate': Decimal(value[7]) / Decimal(10 ** 27),
        'average_stable_rate': Decimal(value[8]) / Decimal(10 ** 27),
        'liquidity_index': Decimal(value[9]) / Decimal(10 ** 27),
        'variable_borrow_index': Decimal(value[10]) / Decimal(10 ** 27),
        'last_update_timestamp': datetime.utcfromtimestamp(value[11]),
        'available_liquidity': (Decimal(value[2]) - Decimal(value[3]) - Decimal(value[4])) / Decimal(10 ** decimals),
    }
def reserve_data_handler(value):
    return {
        'configuration': {
            'ltv': (value[0] & 0xFFFF),
            'liquidation_threshold': (value[0] >> 16) & 0xFFFF,
            'liquidation_bonus': (value[0] >> 32) & 0xFFFF,
            'decimals': (value[0] >> 48) & 0xFF,
            'is_active': bool((value[0] >> 56) & 0x1),
            'is_frozen': bool((value[0] >> 57) & 0x1),
            'borrowing_enabled': bool((value[0] >> 58) & 0x1),
            'stable_borrow_rate_enabled': bool((value[0] >> 59) & 0x1),
            'is_paused': bool((value[0] >> 60) & 0x1),
            'isolation_mode_enabled': bool((value[0] >> 61) & 0x1),
            'reserve_factor': (value[0] >> 64) & 0xFFFF,
            'borrow_cap': (value[0] >> 80) & 0xFFFFFFFF,
            'supply_cap': (value[0] >> 116) & 0xFFFFFFFF,
            'liquidation_protocol_fee': (value[0] >> 152) & 0xFFFF,
            'emode_category': (value[0] >> 168) & 0xFF,
            'unbacked_mint_cap': (value[0] >> 176) & 0xFFFFFFFF,
            'debt_ceiling': (value[0] >> 212) & 0xFFFFFFFF,
        },
        'liquidity_index': Decimal(value[1]) / Decimal(10 ** 27),
        'current_liquidity_rate': Decimal(value[2]) / Decimal(10 ** 27),
        'variable_borrow_index': Decimal(value[3]) / Decimal(10 ** 27),
        'current_variable_borrow_rate': Decimal(value[4]) / Decimal(10 ** 27),
        'current_stable_borrow_rate': Decimal(value[5]) / Decimal(10 ** 27),
        'last_update_timestamp': datetime.utcfromtimestamp(value[6]),
        'id': value[7],
        'aTokenAddress': value[8],
        'stableDebtTokenAddress': value[9],
        'variableDebtTokenAddress': value[10],
        'interestRateStrategyAddress': value[11],
        'accrued_to_treasury': Decimal(value[12]) / Decimal(10 ** 27),
        'unbacked': Decimal(value[13]) / Decimal(10 ** 27),
        'isolation_mode_total_debt': Decimal(value[14]) / Decimal(10 ** 27),
    }

# Create a Call object to get reserve data
reserve_data_call = Call(
    data_provider, 
    ['getReserveData(address)((uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint40))', reserve], 
    [['reserve_data', v3_reserve_data_handler]], 
    _w3=w3,
    block_id=block_height
)
reserve_data_call = Call(
    data_provider,
    ['getReserveData(address)((uint256,uint128,uint128,uint128,uint128,uint128,uint40,uint16,address,address,address,address, uint128, uint128, uint128))', reserve],
    [['reserve_data', lambda v: v]],
    _w3=w3,
    block_id=block_height
)

# Execute the call to get reserve data
reserve_data = reserve_data_call()


# Create a Call object to get the eMode category label
emode_category_id = 1  # Example category ID
emode_category_call = Call(
    pool_address,
    ['getEModeCategoryLabel(uint8)(string)', emode_category_id],
    [['emode_category_label', lambda v: v]],
    _w3=w3,
    block_id=block_height
)

# Execute the call to get the eMode category label
emode_category_label = emode_category_call()



# Create a Call object to get the eMode category data using multicall
emode_category_data_call = Call(
    pool_address,  # Use the pool contract address
    ['getEModeCategoryData(uint8)(uint256,uint256,uint256)', category_id],
    [['emode_category_data', lambda v: v]],  # Output processing
    _w3=w3,
    block_id=block_height
)

# Execute the call to get the eMode category data
emode_category_data = emode_category_data_call()



# Create a Call object to get the eMode category collateral config using multicall
emode_category_collateral_config_call = Call(
    pool_address,  # Use the pool contract address
    ['getEModeCategoryCollateralConfig(uint8)(uint16,uint16,uint16)', category_id],  # Function signature and input
    [['emode_category_collateral_config', lambda v: v]],  # Output processing
    _w3=w3,
    block_id=block_height
)

# Execute the call to get the eMode category collateral config
emode_category_collateral_config = emode_category_collateral_config_call()




# different method
# ABI of the IPool contract
pool_abi = [
    {
        "constant": True,
        "inputs": [{"name": "id", "type": "uint8"}],
        "name": "getEModeCategoryLabel",
        "outputs": [{"name": "", "type": "string"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

# Create contract instance
pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)

# Call the getEModeCategoryLabel function
category_id = 1  # Example category ID
emode_label = pool_contract.functions.getEModeCategoryLabel(category_id).call()




# Updated ABI of the IPool contract to include getEModeCategoryCollateralConfig
pool_abi = [
    {
        "constant": True,
        "inputs": [{"name": "id", "type": "uint8"}],
        "name": "getEModeCategoryLabel",
        "outputs": [{"name": "", "type": "string"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [{"name": "id", "type": "uint8"}],
        "name": "getEModeCategoryCollateralConfig",
        "outputs": [
            {"name": "ltv", "type": "uint16"},
            {"name": "liquidationThreshold", "type": "uint16"},
            {"name": "liquidationBonus", "type": "uint16"}
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

# Create contract instance
pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)

# Call the getEModeCategoryLabel function
category_id = 1  # Example category ID
emode_label = pool_contract.functions.getEModeCategoryLabel(category_id).call()

# Call the getEModeCategoryCollateralConfig function
emode_collateral_config = pool_contract.functions.getEModeCategoryCollateralConfig(category_id).call()
