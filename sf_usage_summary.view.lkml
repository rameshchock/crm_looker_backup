view: sf_usage_summary {
  sql_table_name: snowflake.sf_usage_summary ;;

  dimension: environment {
    type: string
    sql: ${TABLE}.env ;;
  }

  dimension_group: date_id {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.date_id ;;
  }

  measure: bytes_transferred_tb {
    type: sum
    sql: ${TABLE}.bytes_transferred ;;
    value_format_name: decimal_2
  }

  measure: usage_cost {
    type: sum
    sql: ${TABLE}.credit_cost ;;
    value_format_name: usd_0
  }

  measure: credit_used {
    type: sum
    sql: ${TABLE}.credit_used ;;
  }

  measure: data_transfer_cost {
    type: sum
    sql: ${TABLE}.data_transfer_cost ;;
    value_format_name: usd_0
  }

  measure: failsafe_bytes_tb {
    type: average
    sql: ${TABLE}.failsafe_bytes ;;
    value_format_name: decimal_2
  }

  measure: storage_bytes_tb {
    type: average
    sql: ${TABLE}.storage_bytes ;;
    value_format_name: decimal_2
  }

  measure: total_storage_bytes_tb {
    type: average
    sql: ${TABLE}.storage_bytes + ${TABLE}.failsafe_bytes;;
    value_format_name: decimal_2
  }

  measure: storage_cost {
    type: average
    sql: ${TABLE}.storage_cost ;;
    value_format_name: usd_0
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
