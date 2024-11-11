import polars as pl

access_list_struct = pl.Struct([
    pl.Field("address", pl.Utf8),
    pl.Field("storageKeys", pl.List(pl.Utf8))
])

ethereum_schema = {
    "accessList": pl.List(access_list_struct),
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
    "yParity": pl.Utf8
}
