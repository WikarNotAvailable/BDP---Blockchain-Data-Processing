import polars as pl

# Define inner structures according to the specified schema order
inner_instruction_struct = pl.Struct(
    [
        pl.Field("index", pl.Int64),
        pl.Field(
            "instructions",
            pl.List(
                pl.Struct(
                    [
                        pl.Field("programId", pl.Utf8),
                        pl.Field("data", pl.Utf8),
                        pl.Field("accounts", pl.List(pl.Utf8)),
                    ]
                )
            ),
        ),
    ]
)

token_balance_struct = pl.Struct(
    [
        pl.Field("accountIndex", pl.Int64),
        pl.Field("mint", pl.Utf8),
        pl.Field("owner", pl.Utf8),
        pl.Field("programId", pl.Utf8),
        pl.Field(
            "uiTokenAmount",
            pl.Struct(
                [
                    pl.Field("amount", pl.Utf8),
                    pl.Field("decimals", pl.Int64),
                    pl.Field("uiAmount", pl.Float64),
                    pl.Field("uiAmountString", pl.Utf8),
                ]
            ),
        ),
    ]
)

address_table_lookup_struct = pl.Struct(
    [
        pl.Field("accountKey", pl.Utf8),
        pl.Field("readonlyIndexes", pl.List(pl.Int64)),
        pl.Field("writableIndexes", pl.List(pl.Int64)),
    ]
)

instruction_error_struct = pl.Struct(
    [
        pl.Field("index", pl.Int64),
        pl.Field("Custom", pl.Int64),
        pl.Field("StringError", pl.Utf8),
    ]
)

error_struct = pl.Struct(
    [pl.Field("InstructionError", pl.List(instruction_error_struct))]
)

# Nest `meta` and `transaction` fields as dictionaries
solana_schema = {
    "meta": pl.Struct(
        [
            pl.Field("computeUnitsConsumed", pl.Int64),
            pl.Field("err", error_struct),
            pl.Field("fee", pl.Int64),
            pl.Field("innerInstructions", pl.List(inner_instruction_struct)),
            pl.Field(
                "loadedAddresses",
                pl.Struct(
                    [
                        pl.Field("readonly", pl.List(pl.Utf8)),
                        pl.Field("writable", pl.List(pl.Utf8)),
                    ]
                ),
            ),
            pl.Field("logMessages", pl.List(pl.Utf8)),
            pl.Field("postBalances", pl.List(pl.Int64)),
            pl.Field("postTokenBalances", pl.List(token_balance_struct)),
            pl.Field("preBalances", pl.List(pl.Int64)),
            pl.Field("preTokenBalances", pl.List(token_balance_struct)),
            pl.Field(
                "rewards",
                pl.List(
                    pl.Struct(
                        [
                            pl.Field("pubkey", pl.Utf8),
                            pl.Field("lamports", pl.Int64),
                            pl.Field("postBalance", pl.UInt64),
                            pl.Field("rewardType", pl.Utf8),
                            pl.Field("commission", pl.UInt8),
                        ]
                    )
                ),
            ),
            pl.Field(
                "status",
                pl.Struct([pl.Field("Ok", pl.Null), pl.Field("Err", error_struct)]),
            ),
        ]
    ),
    "transaction": pl.Struct(
        [
            pl.Field(
                "message",
                pl.Struct(
                    [
                        pl.Field("accountKeys", pl.List(pl.Utf8)),
                        pl.Field(
                            "header",
                            pl.Struct(
                                [
                                    pl.Field("numReadonlySignedAccounts", pl.Int64),
                                    pl.Field("numReadonlyUnsignedAccounts", pl.Int64),
                                    pl.Field("numRequiredSignatures", pl.Int64),
                                ]
                            ),
                        ),
                        pl.Field(
                            "instructions",
                            pl.List(
                                pl.Struct(
                                    [
                                        pl.Field("accounts", pl.List(pl.Int64)),
                                        pl.Field("data", pl.Utf8),
                                        pl.Field("programIdIndex", pl.Int64),
                                        pl.Field("stackHeight", pl.Utf8),
                                    ]
                                )
                            ),
                        ),
                        pl.Field("recentBlockhash", pl.Utf8),
                        pl.Field(
                            "addressTableLookups", pl.List(address_table_lookup_struct)
                        ),
                    ]
                ),
            ),
            pl.Field("signatures", pl.List(pl.Utf8)),
        ]
    ),
    "version": pl.Utf8,
}

# Nest `meta` and `transaction` fields as dictionaries
bitcoin_schema = {
    "hash": pl.Utf8,
    "ver": pl.Int64,
    "vin_sz": pl.Int64,
    "vout_sz": pl.Int64,
    "size": pl.Int64,
    "weight": pl.Int64,
    "fee": pl.Int64,
    "relayed_by": pl.Utf8,
    "lock_time": pl.Int64,
    "tx_index": pl.Int64,
    "double_spend": pl.Boolean,
    "time": pl.Int64,
    "block_index": pl.Int64,  # or pl.Null if always None
    "block_height": pl.Int64,  # or pl.Null if always None
    "inputs": pl.List(
        pl.Struct(  # Nested structure for "inputs"
            [
                pl.Field("sequence", pl.Int64),
                pl.Field("witness", pl.Utf8),
                pl.Field("script", pl.Utf8),
                pl.Field("index", pl.Int64),
                pl.Field(
                    "prev_out",
                    pl.Struct(
                        [
                            pl.Field("type", pl.Int64),
                            pl.Field("spent", pl.Boolean),
                            pl.Field("value", pl.Int64),
                            pl.Field(
                                "spending_outpoints",
                                pl.List(  # Nested list in "prev_out"
                                    pl.Struct(
                                        [
                                            pl.Field("tx_index", pl.Int64),
                                            pl.Field("n", pl.Int64),
                                        ]
                                    )
                                ),
                            ),
                            pl.Field("n", pl.Int64),
                            pl.Field("tx_index", pl.Int64),
                            pl.Field("script", pl.Utf8),
                            pl.Field("addr", pl.Utf8),
                        ]
                    ),
                ),
            ]
        )
    ),
    "out": pl.List(
        pl.Struct(  # Nested structure for "out"
            [
                pl.Field("type", pl.Int64),
                pl.Field("spent", pl.Boolean),
                pl.Field("value", pl.Int64),
                pl.Field(
                    "spending_outpoints",
                    pl.List(  # Nested list in "prev_out"
                        pl.Struct(
                            [pl.Field("tx_index", pl.Int64), pl.Field("n", pl.Int64)]
                        )
                    ),
                ),
                pl.Field("n", pl.Int64),
                pl.Field("out_tx_index", pl.Int64),
                pl.Field("script", pl.Utf8),
                pl.Field("addr", pl.Utf8),
            ]
        )
    ),
}

ethereum_schema = {
    "accessList": pl.List(
        pl.Struct({"address": pl.Utf8, "storageKeys": pl.List(pl.Utf8)})
    ),
    "blockHash": pl.Utf8,
    "blockNumber": pl.Utf8,
    "chainId": pl.Utf8,
    "from": pl.Utf8,
    "gas": pl.Utf8,
    "gasPrice": pl.Utf8,
    "hash": pl.Utf8,
    "input": pl.Utf8,
    "maxFeePerGas": pl.Utf8,
    "maxPriorityFeePerGas": pl.Utf8,
    "nonce": pl.Utf8,
    "r": pl.Utf8,
    "s": pl.Utf8,
    "to": pl.Utf8,
    "transactionIndex": pl.Utf8,
    "type": pl.Utf8,
    "v": pl.Utf8,
    "value": pl.Utf8,
    "yParity": pl.Utf8,
}
