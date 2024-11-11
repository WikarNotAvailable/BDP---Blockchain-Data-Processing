import polars as pl

solana_blocks_schema = {
    "slot": pl.Int64,
    "block_hash": pl.Utf8,
    "block_timestamp": pl.Datetime,
    "height": pl.Int64,
    "previous_block_hash": pl.Utf8,
    "transaction_count": pl.Int64,
    "leader_reward": pl.Float64,
    "leader": pl.Utf8
}

solana_token_transfers_schema = {
    "block_slot": pl.Int64,
    "block_hash": pl.Utf8,
    "block_timestamp": pl.Datetime,
    "tx_signature": pl.Utf8,
    "source": pl.Utf8,
    "destination": pl.Utf8,
    "authority": pl.Utf8,
    "value": pl.Float64,
    "decimals": pl.Float64,
    "mint": pl.Utf8,
    "mint_authority": pl.Utf8,
    "fee": pl.Float64,
    "fee_decimals": pl.Float64,
    "memo": pl.Utf8,
    "transfer_type": pl.Utf8
}

solana_transactions_schema = {
    "block_slot": pl.Int64,
    "block_hash": pl.Utf8,
    "block_timestamp": pl.Datetime,
    "recent_block_hash": pl.Utf8,
    "signature": pl.Utf8,
    "index": pl.Int64,
    "fee": pl.Float64,
    "status": pl.Utf8,
    "err": pl.Utf8,
    "compute_units_consumed": pl.Float64,
    "accounts": pl.List(pl.Struct([
        pl.Field("pubkey", pl.Utf8),
        pl.Field("signer", pl.Boolean),
        pl.Field("writable", pl.Boolean)
    ])),
    "log_messages": pl.List(pl.Utf8), 
    "balance_changes": pl.List(pl.Struct([
        pl.Field("account", pl.Utf8),
        pl.Field("before", pl.Float64),
        pl.Field("after", pl.Float64)
    ])),
    "pre_token_balances": pl.List(pl.Struct([
        pl.Field("account_index", pl.Int64),
        pl.Field("mint", pl.Utf8),
        pl.Field("owner", pl.Utf8),
        pl.Field("amount", pl.Float64),
        pl.Field("decimals", pl.Int64)
    ])),
    "post_token_balances": pl.List(pl.Struct([
        pl.Field("account_index", pl.Int64),
        pl.Field("mint", pl.Utf8),
        pl.Field("owner", pl.Utf8),
        pl.Field("amount", pl.Float64),
        pl.Field("decimals", pl.Int64)
    ]))
}