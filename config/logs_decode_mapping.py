
# Known event signatures and their types
event_signatures = {
    "TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)": "TransferSingle",
    "ComponentValueSet(uint256 indexed componentId, uint256 indexed entity, bytes data)": "ComponentValueSet",
    "TraitValueSet(address indexed tokenContract, uint256 indexed tokenId, uint256 indexed traitId, bytes value)": "TraitValueSet",
    "Transfer(address indexed from, address indexed to, uint256 value)": "Transfer",
    "PirateTransformCompleted(address account, uint256 transformEntity, uint16 startedCount, uint16 successCount, address nftTokenContract, uint256 nftTokenId)": "PirateTransformCompleted", 
    "RandomNumberDelivered(uint256 indexed requestId, uint256 number)": "RandomNumberDelivered",
    "BattlePending(uint256 indexed battleEntity, uint256 indexed attackerEntity, uint256 indexed defenderEntity, uint256[] attackerOverloads, uint256[] defenderOverloads)": "BattlePending",
    "RequestRandomNumber(uint256 indexed requestId)": "RequestRandomNumber",
    "CountSet(uint256 entity, uint256 key, uint256 newTotal)": "CountSet",
    "DungeonLootGranted(address indexed account, uint256 indexed battleEntity, uint256 scheduledStartTimestamp, uint256 mapEntity, uint256 node)": "DungeonLootGranted",
    "UpgradePirateLevel(address indexed tokenContract, uint256 indexed tokenId, uint256 newLevel)": "UpgradePirateLevel",
    "ComponentValueRemoved(uint256 indexed componentId, uint256 indexed entity)": "ComponentValueRemoved", 
    "PerformGameItemAction(address account, address tokenContract, uint256 tokenId, uint256 amount, uint256 actionId)": "PerformGameItemAction",
}


# Precompute the hashes of the event signatures
event_hashes = {
    '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62': "TransferSingle",
    '0x6837901feacdbb2b5f689b180c02268b287523b334088077ba4c35daf4fe34a8': "ComponentValueSet",
    '0x161f549f01144f89b39ecb5813ffb68eb7d96745f0670fd34d54edfc69c6cd8f': "TraitValueSet",
    '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef': "Transfer",
    '0xb5646821f44ee053a85b0a7363ee209ee3f7bb38e035c63a89b0ac31532200b8': "PirateTransformCompleted", 
    '0xd0b427fb20acc371e4ce46f89bd392aee2eeee70b9c694912ebb4e0e77512e0f': "RandomNumberDelivered",
    '0xecdda95e7be519890afb8d7a6066c6b0f4b22402acf73ca3b86bcc3ba3ad14fe': "BattlePending",
    '0x41eb194f8e72283e36363014be614f33204ce6cb2ede1163474be91f4d8f3cd9': "RequestRandomNumber",
    '0x07b139a03d6e8fe0000625b64d0583aaf9be7ddc825d4891c3c92f4601910735': "CountSet",
    '0xce6a9b23ff5c8e30981b3176e3e10ef81cc623a9fd77d04f125e3b109a6c49c2': "DungeonLootGranted",
    '0xfdeeaf66101caa3b9381f67b2283c02a2ce9bb45fbe01599bc502ecf3cd65dcb': "UpgradePirateLevel",
    '0x8989859870cc0d47cc5f8909459290e411f855f5535f7dbfaf1038cf40fccc0e': "ComponentValueRemoved",
    '0x2f6a39e9fc9144751ab2648c46e182b869a591ded4a2b760b8c8b8b179cf6aa7': "PerformGameItemAction",
}

