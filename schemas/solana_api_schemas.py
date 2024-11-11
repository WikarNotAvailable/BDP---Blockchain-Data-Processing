import polars as pl


inner_instruction_struct = pl.Struct([
    pl.Field("index", pl.Int64),
    pl.Field("instructions", pl.List(pl.Struct([
        pl.Field("programId", pl.Utf8),
        pl.Field("data", pl.Utf8),
        pl.Field("accounts", pl.List(pl.Utf8))
    ])))
])

token_balance_struct = pl.Struct([
    pl.Field("accountIndex", pl.Int64),
    pl.Field("mint", pl.Utf8),
    pl.Field("owner", pl.Utf8),
    pl.Field("programId", pl.Utf8), 
    pl.Field("uiTokenAmount", pl.Struct([
        pl.Field("amount", pl.Utf8),
        pl.Field("decimals", pl.Int64), 
        pl.Field("uiAmount", pl.Float64), 
        pl.Field("uiAmountString", pl.Utf8) 
    ]))
])

address_table_lookup_struct = pl.Struct([
    pl.Field("accountKey", pl.Utf8),
    pl.Field("readonlyIndexes", pl.List(pl.Int64)),
    pl.Field("writableIndexes", pl.List(pl.Int64))
])

solana_schema = {
    "version": pl.Utf8,
    "meta.computeUnitsConsumed": pl.Int64,
    "meta.err": pl.Utf8,
    "meta.fee": pl.Int64,
    "meta.innerInstructions": pl.List(inner_instruction_struct), 
    "meta.loadedAddresses.readonly": pl.List(pl.Utf8),
    "meta.loadedAddresses.writable": pl.List(pl.Utf8),
    "meta.logMessages": pl.List(pl.Utf8),
    "meta.postBalances": pl.List(pl.Int64),
    "meta.postTokenBalances": pl.List(token_balance_struct), 
    "meta.preBalances": pl.List(pl.Int64),
    "meta.preTokenBalances": pl.List(token_balance_struct), 

    "meta.rewards": pl.List(pl.Struct([
        pl.Field("pubkey", pl.Utf8),
        pl.Field("lamports", pl.Int64),
        pl.Field("postBalance", pl.UInt64),
        pl.Field("rewardType", pl.Utf8),
        pl.Field("commission", pl.UInt8)
    ])),

    "meta.status.Ok": pl.Struct([
        pl.Field("Ok", pl.Null),
        pl.Field("Err", pl.Utf8)
    ]),

    "transaction.message.accountKeys": pl.List(pl.Utf8),
    "transaction.message.header.numReadonlySignedAccounts": pl.Int64,
    "transaction.message.header.numReadonlyUnsignedAccounts": pl.Int64,
    "transaction.message.header.numRequiredSignatures": pl.Int64,
    
    "transaction.message.instructions": pl.List(pl.Struct([
        pl.Field("accounts", pl.List(pl.Int64)),
        pl.Field("data", pl.Utf8),
        pl.Field("programIdIndex", pl.Int64),
        pl.Field("stackHeight", pl.Utf8)
    ])),
    
    "transaction.message.recentBlockhash": pl.Utf8,
    "transaction.signatures": pl.List(pl.Utf8),
    "transaction.message.addressTableLookups": pl.List(address_table_lookup_struct),
    
    "meta.returnData.data": pl.Struct([
        pl.Field("encodedData", pl.Utf8),
        pl.Field("encoding", pl.Utf8) 
    ]),

    "meta.returnData.programId": pl.Utf8,
    "meta.err.InstructionError": pl.Utf8,
    "meta.status.Err.InstructionError": pl.Utf8,
    "meta.err.InsufficientFundsForRent.account_index": pl.Utf8,
    "meta.status.Err.InsufficientFundsForRent.account_index": pl.Utf8,
}
