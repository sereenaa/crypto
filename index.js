import dotenv from 'dotenv'; // Ensure dotenv is imported
import { PublicKey, Keypair, Connection, VersionedTransaction, TransactionInstruction, TransactionMessage } from '@solana/web3.js';
import { TOKEN_2022_PROGRAM_ID, burnChecked, createBurnCheckedInstruction, createTransferCheckedInstruction, getAssociatedTokenAddressSync, getMint, getTransferFeeAmount, transferChecked, unpackAccount, unpackMint, withdrawWithheldTokensFromAccounts } from "@solana/spl-token";
import { SolanaFMParser, checkIfAccountParser, ParserType } from "@solanafm/explorer-kit";
import { getProgramIdl } from "@solanafm/explorer-kit-idls";

dotenv.config(); // Load environment variables

const LOCK_PROGRAM = 'LocktDzaV1W2Bm9DeZeiyz4J9zs4fRqNiYqQyracRXw';
const LOCKED_ADDRESS = '8erad8kmNrLJDJPe9UkmTHomrMV3EW48sjGeECyVjbYX';

async function fetchDataAndUpload() {
    try {
        const SFMIdlItem = await getProgramIdl(LOCK_PROGRAM);
        const SBR_ADDRESS = 'Saber2gLauYim4Mvftnrasomsv6NvAuncvMEZwcLpD1';
        console.log('SBR_RPC:', process.env.SBR_RPC);
        const connection = new Connection(process.env.SBR_RPC, 'processed');
        const mint = await getMint(connection, new PublicKey(SBR_ADDRESS));

        const accounts = await connection.getParsedProgramAccounts(new PublicKey(LOCK_PROGRAM));
        const parser = new SolanaFMParser(SFMIdlItem);
        const eventParser = parser.createParser(ParserType.ACCOUNT);

        if (!eventParser || !checkIfAccountParser(eventParser)) {
            throw Error('nope');
        }

        const decodedData = accounts
            .map(account => {
                try {
                    return eventParser.parseAccount(Buffer.from(account.account.data).toString('base64'));
                } catch (e) {
                    return undefined;
                }
            })
            .filter(account => account && account.name === 'Escrow' && account.data.locker === LOCKED_ADDRESS);

        const veSbrSupply = decodedData.reduce((acc, val) => {
            const sbr = parseFloat(val.data.amount) / 10 ** 6;
            const vesbr = Math.max(0, sbr * (parseInt(val.data.escrowEndsAt) - Math.round(Date.now() / 1000)) / (365 * 5 * 86400) * 10);
            return acc + vesbr;
        }, 0);

        const lockedAmount = decodedData.reduce((acc, val) => acc + parseFloat(val.data.amount) / 10 ** 6, 0);
        const supply = Number(mint.supply) / 10 ** mint.decimals;

        const result = {
            circulating: `${supply - lockedAmount}`,
            supply: `${supply}`,
            lockedAmount: `${lockedAmount}`,
            veSbrSupply: `${veSbrSupply}`,
            decodedData
        };

        // Prepare CSV content
        const csvContent = `circulating,supply,lockedAmount,veSbrSupply\n${result.circulating},${result.supply},${result.lockedAmount},${result.veSbrSupply}`;
        console.log(csvContent);

        // Upload CSV to Dune
        const duneApiKey = process.env.DUNE_API_KEY; // Ensure you have this in your .env file
        await uploadCsvToDune(duneApiKey, csvContent, 'saber', 'circulation_data_for_saber');

        return true;
    } catch (error) {
        console.error('Error in fetchDataAndUpload:', error);
        throw error; // Ensure Lambda knows it failed
    }
}


// Function to upload CSV to Dune
async function uploadCsvToDune(apiKey, csvData, tableName, description) {
    const options = {
        method: 'POST',
        headers: {
            'X-DUNE-API-KEY': apiKey,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            data: csvData,
            table_name: tableName,
            description: description,
            is_private: false
        })
    };
    
    try {
        const response = await fetch('https://api.dune.com/api/v1/table/upload/csv', options);
        const data = await response.json();
        console.log(data);
    } catch (err) {
        console.error('Error uploading CSV to Dune:', err);
    }
}

// Execute the main function
// fetchDataAndUpload().catch(err => {
//     console.error(err);
// });

export const handler = async (event, context) => {
    try {
        const result = await fetchDataAndUpload();
        return {
            statusCode: 200,
            body: JSON.stringify({ success: result }),
        };
    } catch (error) {
        console.error('Error in Lambda handler:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: 'Internal Server Error' }),
        };
    }
};
