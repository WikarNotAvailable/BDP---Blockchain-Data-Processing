import json
import logging
import os
from datetime import datetime

import polars as pl
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

from schemes import bitcoin_schema, ethereum_schema

# Load environment variables
load_dotenv()

# Constants
BATCH_SIZE = 5000
LOG_FILE_NAME = "data_collection.log"
DRY_RUN = False

# API Keys
API_KEYS = {
    "infura": [os.getenv("INFURA_KEY_1"), os.getenv("INFURA_KEY_2")],
}

# Scheduling Intervals
SCHEDULING_INTERVALS = {
    "bitcoin": 5,  # minutes
    "ethereum": 10,
}

# Global Variables
current_keys = {
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


if __name__ == "__main__":
    logging.info(f"PID: {os.getpid()}")
    # Initialize data containers
    bitcoin_batch_data, ethereum_batch_data = [], []
    bitcoin_batch_number = {"value": 1}
    ethereum_batch_number = {"value": 1}

    # Initialize data sizes
    data_sizes = {
        network: {
            compression: {"value": 0}
            for compression in ["snappy", "gzip", "zstd", "json"]
        }
        for network in ["bitcoin", "ethereum"]
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

    print("Press Ctrl+{0} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
