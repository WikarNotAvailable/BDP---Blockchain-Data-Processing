transaction_numeric_cols = [
    "block_number", 
    "transaction_index", 
    "fee", 
    "total_transferred_value", 
    "total_input_value", 
    "sent_value", 
    "received_value"
]

aggregation_numeric_cols = [
    "avg_sent_value",
    "avg_received_value",
    "avg_total_value_for_sender",
    "avg_total_value_for_receiver",
    "sum_sent_value",
    "sum_received_value",
    "sum_total_value_for_sender",
    "sum_total_value_for_receiver",
    "min_sent_value",
    "min_received_value",
    "min_total_value_for_sender",
    "min_total_value_for_receiver",
    "max_sent_value",
    "max_received_value",
    "max_total_value_for_sender",
    "max_total_value_for_receiver",
    "median_sent_value",
    "median_received_value",
    "median_total_value_for_sender",
    "median_total_value_for_receiver",
    "mode_sent_value",
    "mode_received_value",
    "mode_total_value_for_sender",
    "mode_total_value_for_receiver",
    "stddev_sent_value",
    "stddev_received_value",
    "stddev_total_value_for_sender",
    "stddev_total_value_for_receiver",
    "num_sent_transactions",
    "num_received_transactions",
    "avg_time_between_sent_transactions",
    "avg_time_between_received_transactions",
    "avg_outgoing_speed_count",
    "avg_incoming_speed_count",
    "avg_outgoing_speed_value",
    "avg_incoming_speed_value",
    "avg_outgoing_acceleration_count",
    "avg_incoming_acceleration_count",
    "avg_outgoing_acceleration_value",
    "avg_incoming_acceleration_value",
    "avg_fee_paid",
    "total_fee_paid",
    "min_fee_paid",
    "max_fee_paid",
    "activity_duration",
    "unique_out_degree",
    "unique_in_degree"
]
transactions_datetime_cols = [
    "block_timestamp"
]

aggregations_datetime_cols = [
    "first_transaction_timestamp",
    "last_transaction_timestamp"
]

wallets_aggregations_string_cols = [
    "transaction_id",
    "transaction_hash",
    "sender_address",
    "receiver_address",
    "network_name"
]

sender_fields = [
    "avg_received_value",
    "avg_total_value_for_receiver",
    "sum_received_value",
    "sum_total_value_for_receiver",
    "min_received_value",
    "min_total_value_for_receiver",
    "max_received_value",
    "max_total_value_for_receiver",
    "median_received_value",
    "median_total_value_for_receiver",
    "mode_received_value",
    "mode_total_value_for_receiver",
    "stddev_received_value",
    "stddev_total_value_for_receiver",
    "num_received_transactions",
    "avg_time_between_received_transactions",
    "avg_incoming_speed_count",
    "avg_incoming_speed_value",
    "avg_incoming_acceleration_count",
    "avg_incoming_acceleration_value",
    "unique_in_degree",
    "avg_fee_paid",
    "total_fee_paid",
    "min_fee_paid",
    "max_fee_paid",
]

receiver_fields = [
    "avg_sent_value",
    "avg_total_value_for_sender",
    "sum_sent_value",
    "sum_total_value_for_sender",
    "min_sent_value",
    "min_total_value_for_sender",
    "max_sent_value",
    "max_total_value_for_sender",
    "median_sent_value",
    "median_total_value_for_sender",
    "mode_sent_value",
    "mode_total_value_for_sender",
    "stddev_sent_value",
    "stddev_total_value_for_sender",
    "num_sent_transactions",
    "avg_time_between_sent_transactions",
    "avg_outgoing_speed_count",
    "avg_outgoing_speed_value",
    "avg_outgoing_acceleration_count",
    "avg_outgoing_acceleration_value",
    "unique_out_degree",
]

common_fields = [
    "address",
    "activity_duration",
    "first_transaction_timestamp",
    "last_transaction_timestamp"
]
