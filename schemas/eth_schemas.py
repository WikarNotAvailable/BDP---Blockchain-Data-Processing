import polars as pl

eth_blocks_schema = {
    "data": pl.UInt8,
    "timestamp": pl.Datetime,
    "number": pl.Int64,
    "hash": pl.Utf8,
    "parent_hash": pl.Utf8,
    "nonce": pl.Utf8,
    "sha3_uncles": pl.Utf8,
    "logs_bloom": pl.Utf8,
    "transactions_root": pl.Utf8,
    "state_root": pl.Utf8,
    "receipts_root": pl.Utf8,
    "miner": pl.Utf8,
    "difficulty": pl.Float64,
    "total_difficulty": pl.Float64,
    "size": pl.Int64,
    "extra_data": pl.Utf8,
    "gas_limit": pl.Int64,
    "gas_used": pl.Int64,
    "transaction_count": pl.Int64,
    "base_fee_per_gas": pl.Int64
}

eth_transactions_schema = {
    "data": pl.Utf8,
    "hash": pl.Utf8,
    "nonce": pl.Int64,
    "transaction_index": pl.Int64,
    "from_address": pl.Utf8,
    "to_address": pl.Utf8,
    "value": pl.float64,
    "gas": pl.Int64,
    "gas_price": pl.Int64,
    "input": pl.Utf8,
    "receipt_cumulative_gas_used": pl.Int64,
    "receipt_gas_used": pl.Int64,
    "receipt_contract_address": pl.Utf8,
    "receipt_status": pl.Int64,
    "block_timestamp": pl.Datetime,
    "block_number": pl.Int64,
    "block_hash": pl.Utf8,
    "max_fee_per_gas": pl.Int64,
    "max_priority_fee_per_gas": pl.Int64,
    "transaction_type": pl.Int64,
    "receipt_effective_gas_price": pl.Int64
}

eth_token_transfers_schema = {
    "date": pl.Utf8,
    "token_address": pl.Utf8,
    "from_address": pl.Utf8,
    "to_address": pl.Utf8,
    "value": pl.Float64,
    "transaction_hash": pl.Utf8,
    "log_index": pl.Int64,
    "block_timestamp": pl.Datetime,
    "block_number": pl.Int64,
    "block_hash": pl.Utf8
}
