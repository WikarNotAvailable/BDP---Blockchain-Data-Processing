import polars as pl

input_struct = pl.Struct([
    pl.Field("sequence", pl.Int64),
    pl.Field("witness", pl.Utf8),
    pl.Field("script", pl.Utf8),
    pl.Field("index", pl.Int64),
    pl.Field("prev_out", pl.Struct([
        pl.Field("type", pl.Int64),
        pl.Field("spent", pl.Boolean),
        pl.Field("value", pl.Int64),
        pl.Field("spending_outpoints", pl.List(pl.Struct([
            pl.Field("tx_index", pl.Int64),
            pl.Field("n", pl.Int64)
        ]))),
        pl.Field("n", pl.Int64),
        pl.Field("tx_index", pl.Int64),
        pl.Field("script", pl.Utf8),
        pl.Field("addr", pl.Utf8)
    ]))
])

output_struct = pl.Struct([
    pl.Field("type", pl.Int64),
    pl.Field("spent", pl.Boolean),
    pl.Field("value", pl.Int64),
    pl.Field("spending_outpoints", pl.List(pl.Struct([
        pl.Field("tx_index", pl.Int64),
        pl.Field("n", pl.Int64)
    ]))),
    pl.Field("n", pl.Int64),
    pl.Field("tx_index", pl.Int64),
    pl.Field("script", pl.Utf8),
    pl.Field("addr", pl.Utf8)
])

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
    "block_index": pl.Null,
    "block_height": pl.Null,
    "inputs": pl.List(input_struct),
    "out": pl.List(output_struct)
}