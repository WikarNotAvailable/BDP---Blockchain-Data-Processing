import json
import logging
import os
from datetime import datetime

import polars as pl
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

from schemes import bitcoin_schema, ethereum_schema, solana_schema

# Load environment variables
load_dotenv()

# Constants
BATCH_SIZE = 5000
LOG_FILE_NAME = "data_collection.log"
DRY_RUN = False

# API Keys
API_KEYS = {
    "alchemy": [os.getenv("ALCHEMY_KEY_1"), os.getenv("ALCHEMY_KEY_2")],
    "infura": [os.getenv("INFURA_KEY_1"), os.getenv("INFURA_KEY_2")],
}

# Scheduling Intervals
SCHEDULING_INTERVALS = {
    "bitcoin": 5,  # minutes
    "ethereum": 10,
    "solana": 2,
}

# Global Variables
current_keys = {
    "alchemy": API_KEYS["alchemy"][0],
    "infura": API_KEYS["infura"][0],
}

# Set up logging
logging.basicConfig(
    filename=LOG_FILE_NAME,
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def log_error(message: str, error: Exception):
    logging.error(f"{message}: {error}")


def save_batch(
    batch_json,
    batch_data: pl.DataFrame,
    batch_number: int,
    network: str,
    data_sizes: dict,
    dry_run: bool = False,
):
    output_dir = "data_batches"
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f")[:-3]
    filenames = {
        "snappy": f"snappy_{network}_{timestamp}_batch_{batch_number}.parquet",
        "gzip": f"gzip_{network}_{timestamp}_batch_{batch_number}.parquet",
        "zstd": f"zstd_{network}_{timestamp}_batch_{batch_number}.parquet",
    }

    try:
        data_sizes["json"]["value"] += len(json.dumps(batch_json))

        for compression in filenames:
            filepath = os.path.join(output_dir, filenames[compression])
            batch_data.write_parquet(
                filepath, compression=compression, use_pyarrow=True
            )

            data_sizes[compression]["value"] += os.path.getsize(filepath)

        logging.info(
            f"Saved batch {batch_number} with {len(batch_data)} records to {filenames['snappy']}"
        )

        for entity, size_info in data_sizes.items():
            logging.info(
                f"Current data size for {network} using {entity} compression: {size_info['value']} bytes."
            )

        if dry_run:
            for filepath in filenames.values():
                os.remove(filepath)

    except Exception as e:
        log_error(f"Failed to save batch {batch_number}", e)


# Bitcoin Processing
def get_last_txs_bitcoin() -> list:
    response = requests.get(
        "https://blockchain.info/unconfirmed-transactions?format=json"
    )
    if response.status_code == 200:
        return response.json()["txs"]
    else:
        logging.error(
            f"Failed to retrieve Bitcoin data. Status code: {response.status_code}"
        )
        return []


def process_bitcoin_transactions(output: list, batch_number: dict, data_sizes: dict):
    transactions = get_last_txs_bitcoin()

    logging.info("Last 100 Bitcoin transactions:")
    if len(output) + len(transactions) > BATCH_SIZE:
        try:
            new_to_batch = transactions[: (BATCH_SIZE - len(output))]
            output.extend(new_to_batch)

            tx_df = pl.DataFrame(output, strict=False, schema=bitcoin_schema)
            save_batch(
                output, tx_df, batch_number["value"], "bitcoin", data_sizes, DRY_RUN
            )

            output = transactions[-(len(transactions) - BATCH_SIZE + len(output)) :]
            batch_number["value"] += 1
        except Exception as e:
            log_error("Error while creating Bitcoin DataFrame", e)

    for tx in transactions:
        output.append(tx)
        logging.info(f"[Bitcoin] Transaction ID: {tx['hash']}")


# Ethereum Processing
def get_latest_ethereum_block() -> dict:
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": ["latest", True],
        "id": 1,
    }
    response = requests.post(
        f"https://mainnet.infura.io/v3/{current_keys['infura']}", json=payload
    )

    if response.status_code == 200:
        return response.json()
    else:
        log_error("Failed to retrieve Ethereum data", response.status_code)
        return {}


def process_ethereum_transactions(output: list, batch_number: dict, data_sizes: dict):
    block = get_latest_ethereum_block()

    if "result" in block and block["result"]["transactions"]:
        block_timestamp = block["result"]["timestamp"]

        for transaction in block["result"]["transactions"]:
                transaction["timestamp"] = block_timestamp   
                
        if len(block["result"]["transactions"]) + len(output) > BATCH_SIZE:
            try:
                new_to_batch = block["result"]["transactions"][
                    : (BATCH_SIZE - len(output))
                ]
                output.extend(new_to_batch)

                tx_df = pl.DataFrame(output, strict=False, schema=ethereum_schema)
                save_batch(
                    output,
                    tx_df,
                    batch_number["value"],
                    "ethereum",
                    data_sizes,
                    DRY_RUN,
                )

                output = block["result"]["transactions"][
                    -(len(block["result"]["transactions"]) - BATCH_SIZE + len(output)) :
                ]
                batch_number["value"] += 1
            except Exception as e:
                log_error("Error while creating Ethereum DataFrame", e)

        output.extend(block["result"]["transactions"])
        logging.info(f"[Ethereum] Latest block: {block['result']['hash']}")
    else:
        logging.error("[INFURA] Changing API key.")
        current_keys["infura"] = (
            API_KEYS["infura"][1]
            if current_keys["infura"] == API_KEYS["infura"][0]
            else API_KEYS["infura"][0]
        )


# Solana Processing
def get_solana_blocks() -> list:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
    }
    response = requests.post(
        f"https://solana-mainnet.g.alchemy.com/v2/{current_keys['alchemy']}",
        json=payload,
    )

    if response.status_code == 200 and "result" in response.json():
        slot_number = response.json()["result"]
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlocks",
            "params": [slot_number - 4, slot_number],
        }
        response = requests.post(
            f"https://solana-mainnet.g.alchemy.com/v2/{current_keys['alchemy']}",
            json=payload,
        )

        if response.status_code == 200 and "result" in response.json():
            slots = response.json()["result"]

            payload = [
                {
                    "jsonrpc": "2.0",
                    "id": i + 1,
                    "method": "getBlock",
                    "params": [
                        slot,
                        {
                            "encoding": "json",
                            "maxSupportedTransactionVersion": 0,
                            "transactionDetails": "full",
                            "rewards": False,
                        },
                    ],
                }
                for i, slot in enumerate(slots)
            ]

            response = requests.post(
                f"https://solana-mainnet.g.alchemy.com/v2/{current_keys['alchemy']}",
                json=payload,
            )

            if response.status_code == 200:
                blocks_info = response.json()
                return blocks_info

    logging.error("Failed to retrieve Solana blocks")
    return []


def process_solana_transactions(output: list, batch_number: dict, data_sizes: dict):
    blocks = get_solana_blocks()
    logging.info(f"Latest {len(blocks)} blocks on Solana network:")

    for block in blocks:
        if "result" in block:
            block_timestamp = block["result"]["blockTime"]

            for transaction in block["result"]["transactions"]:
                transaction["timestamp"] = block_timestamp
                
            if len(block["result"]["transactions"]) + len(output) > BATCH_SIZE:
                try:
                    new_to_batch = block["result"]["transactions"][
                        : (BATCH_SIZE - len(output))
                    ]
                    output.extend(new_to_batch)

                    tx_df = pl.DataFrame(output, strict=False, schema=solana_schema)
                    tx_df = tx_df.unnest(columns=["meta", "transaction"])
                    save_batch(
                        output,
                        tx_df,
                        batch_number["value"],
                        "solana",
                        data_sizes,
                        DRY_RUN,
                    )

                    output = block["result"]["transactions"][
                        -(
                            len(block["result"]["transactions"])
                            - BATCH_SIZE
                            + len(output)
                        ) :
                    ]
                    batch_number["value"] += 1
                except Exception as e:
                    log_error("Error while creating Solana DataFrame", e)

            output.extend(block["result"]["transactions"])
            logging.info(f"[Solana] Block hash: {block['result']['blockhash']}")
            logging.info(
                f"[Solana] Number of transactions: {len(block['result']['transactions'])}"
            )
        else:
            logging.error("[ALCHEMY] Changing API key.")
            current_keys["alchemy"] = (
                API_KEYS["alchemy"][1]
                if current_keys["alchemy"] == API_KEYS["alchemy"][0]
                else API_KEYS["alchemy"][0]
            )


if __name__ == "__main__":
    logging.info(f"PID: {os.getpid()}")
    # Initialize data containers
    bitcoin_batch_data, ethereum_batch_data, solana_batch_data = [], [], []
    bitcoin_batch_number = {"value": 1}
    ethereum_batch_number = {"value": 1}
    solana_batch_number = {"value": 1}

    # Initialize data sizes
    data_sizes = {
        network: {
            compression: {"value": 0}
            for compression in ["snappy", "gzip", "zstd", "json"]
        }
        for network in ["bitcoin", "ethereum", "solana"]
    }

    # Schedule jobs
    scheduler = BlockingScheduler()
    scheduler.add_job(
        process_bitcoin_transactions,
        "cron",
        [bitcoin_batch_data, bitcoin_batch_number, data_sizes["bitcoin"]],
        minute=f"*/{SCHEDULING_INTERVALS['bitcoin']}",
    )
    scheduler.add_job(
        process_ethereum_transactions,
        "cron",
        [ethereum_batch_data, ethereum_batch_number, data_sizes["ethereum"]],
        second=f"*/{SCHEDULING_INTERVALS['ethereum']}",
    )
    scheduler.add_job(
        process_solana_transactions,
        "cron",
        [solana_batch_data, solana_batch_number, data_sizes["solana"]],
        second=f"*/{SCHEDULING_INTERVALS['solana']}",
    )

    print("Press Ctrl+{0} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
