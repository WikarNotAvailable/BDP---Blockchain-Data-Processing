import polars as pl

btn_blocks_schema = {
    "data": pl.Utf8,
    "hash": pl.Utf8,
    "size": pl.Int64,
    "stripped_size": pl.Int64,
    "weight": pl.Int64,
    "number": pl.Int64,
    "version": pl.Int32,
    "merkle_root": pl.Utf8,
    "timestamp": pl.Datetime,
    "nonce": pl.Int64,
    "bits": pl.Utf8,
    "coinbase_param": pl.Utf8,
    "transaction_count": pl.Int64,
    "mediantime": pl.Datetime,
    "difficulty": pl.Float64,
    "chainwork": pl.Utf8,
    "previousblockhash": pl.Utf8
}

input_struct = pl.Struct([
    pl.Field("index", pl.Int64),
    pl.Field("spent_transaction_hash", pl.Utf8),
    pl.Field("spent_output_index", pl.Int64),
    pl.Field("script_asm", pl.Utf8),
    pl.Field("script_hex", pl.Utf8),
    pl.Field("sequence", pl.Int64),
    pl.Field("required_signatures", pl.Int64),
    pl.Field("type", pl.Utf8),
    pl.Field("address", pl.Utf8),
    pl.Field("value", pl.Float64)
])

output_struct = pl.Struct([
    pl.Field("index", pl.Int64),
    pl.Field("script_asm", pl.Utf8),
    pl.Field("script_hex", pl.Utf8),
    pl.Field("required_signatures", pl.Int64),
    pl.Field("type", pl.Utf8),
    pl.Field("address", pl.Utf8),
    pl.Field("value", pl.Float64)
])

btn_transactions_schema = {
    "data": pl.Utf8,
    "hash": pl.Utf8,
    "size": pl.Int64,
    "virtual_size": pl.Int64,
    "version": pl.Int64,
    "lock_time": pl.Int64,
    "block_hash": pl.Utf8,
    "block_number": pl.UInt64,
    "block_timestamp": pl.Datetime,
    "index": pl.UInt64,
    "input_count": pl.UInt64,
    "output_count": pl.UInt64,
    "input_value": pl.Float64,
    "output_value": pl.Float64,
    "is_coinbase": pl.Boolean,
    "fee": pl.Float64,
    "inputs": pl.List(input_struct),
    "outputs": pl.List(output_struct),
}