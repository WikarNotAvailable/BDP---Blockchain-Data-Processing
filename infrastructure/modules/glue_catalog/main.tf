resource "aws_glue_catalog_database" "bdp_db" {
  name = "bdp"
}

resource "aws_glue_catalog_table" "aggregated_transactions" {
  database_name = aws_glue_catalog_database.bdp_db.name
  name          = "aggregated_transactions"

  table_type = "EXTERNAL_TABLE"
  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
    }
  }

  partition_keys {
    name = "network_name"
    type = "string"
  }

  partition_keys {
    name = "partition_date"
    type = "string"
  }
  storage_descriptor {
    location      = "s3://bdp-wallets-aggregations"
    input_format  = "org.apache.hadoop.mapred.FileInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "aggregated_transactions_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "address"
      type = "string"
    }
    columns {
      name = "network_name"
      type = "string"
    }
    columns {
      name = "avg_sent_value"
      type = "double"
    }
    columns {
      name = "avg_received_value"
      type = "double"
    }
    columns {
      name = "avg_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "avg_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "sum_sent_value"
      type = "double"
    }
    columns {
      name = "sum_received_value"
      type = "double"
    }
    columns {
      name = "sum_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "sum_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "min_sent_value"
      type = "double"
    }
    columns {
      name = "min_received_value"
      type = "double"
    }
    columns {
      name = "min_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "min_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "max_sent_value"
      type = "double"
    }
    columns {
      name = "max_received_value"
      type = "double"
    }
    columns {
      name = "max_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "max_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "median_sent_value"
      type = "double"
    }
    columns {
      name = "median_received_value"
      type = "double"
    }
    columns {
      name = "median_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "median_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "mode_sent_value"
      type = "double"
    }
    columns {
      name = "mode_received_value"
      type = "double"
    }
    columns {
      name = "mode_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "mode_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "stddev_sent_value"
      type = "double"
    }
    columns {
      name = "stddev_received_value"
      type = "double"
    }
    columns {
      name = "stddev_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "stddev_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "num_sent_transactions"
      type = "bigint"
    }
    columns {
      name = "num_received_transactions"
      type = "bigint"
    }
    columns {
      name = "avg_time_between_sent_transactions"
      type = "double"
    }
    columns {
      name = "avg_time_between_received_transactions"
      type = "double"
    }
    columns {
      name = "avg_outgoing_speed_count"
      type = "double"
    }
    columns {
      name = "avg_incoming_speed_count"
      type = "double"
    }
    columns {
      name = "avg_outgoing_speed_value"
      type = "double"
    }
    columns {
      name = "avg_incoming_speed_value"
      type = "double"
    }
    columns {
      name = "avg_outgoing_acceleration_count"
      type = "double"
    }
    columns {
      name = "avg_incoming_acceleration_count"
      type = "double"
    }
    columns {
      name = "avg_outgoing_acceleration_value"
      type = "double"
    }
    columns {
      name = "avg_incoming_acceleration_value"
      type = "double"
    }
    columns {
      name = "avg_fee_paid"
      type = "double"
    }
    columns {
      name = "total_fee_paid"
      type = "double"
    }
    columns {
      name = "min_fee_paid"
      type = "double"
    }
    columns {
      name = "max_fee_paid"
      type = "double"
    }
    columns {
      name = "activity_duration"
      type = "bigint"
    }
    columns {
      name = "first_transaction_timestamp"
      type = "timestamp"
    }
    columns {
      name = "last_transaction_timestamp"
      type = "timestamp"
    }
    columns {
      name = "unique_out_degree"
      type = "bigint"
    }
    columns {
      name = "unique_in_degree"
      type = "bigint"
    }

  }
}


resource "aws_glue_catalog_table" "cleaned_transactions" {
  database_name = aws_glue_catalog_database.bdp_db.name
  name          = "cleaned_transactions"
  table_type    = "EXTERNAL_TABLE"

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
    }
  }

  partition_keys {
    name = "network_name"
    type = "string"
  }

  partition_keys {
    name = "partition_date"
    type = "string"
  }

  parameters = {
    "write.format.default"            = "parquet"
    "write.parquet.compression-codec" = "zstd"
  }

  storage_descriptor {
    location      = "s3://bdp-cleaned-transactions"
    input_format  = "org.apache.hadoop.mapred.FileInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "cleaned_transactions_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }


    columns {
      name = "transaction_id"
      type = "string"
    }
    columns {
      name = "block_timestamp"
      type = "timestamp"
    }
    columns {
      name = "block_number"
      type = "bigint"
    }
    columns {
      name = "transaction_hash"
      type = "string"
    }
    columns {
      name = "transaction_index"
      type = "bigint"
    }
    columns {
      name = "fee"
      type = "double"
    }
    columns {
      name = "sender_address"
      type = "string"
    }
    columns {
      name = "receiver_address"
      type = "string"
    }
    columns {
      name = "total_transferred_value"
      type = "double"
    }
    columns {
      name = "total_input_value"
      type = "double"
    }
    columns {
      name = "sent_value"
      type = "double"
    }
    columns {
      name = "received_value"
      type = "double"
    }
    columns {
      name = "network_name"
      type = "string"
    }
  }
}
