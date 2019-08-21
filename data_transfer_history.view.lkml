view: data_transfer_history {
  sql_table_name: snowflake.v_data_transfer_history ;;

  dimension_group: end {
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
    sql: ${TABLE}.end_time ;;
  }

  dimension: env {
    type: string
    sql: ${TABLE}.env ;;
  }

  dimension: source_cloud {
    type: string
    sql: ${TABLE}.source_cloud ;;
  }

  dimension: source_region {
    type: string
    sql: ${TABLE}.source_region ;;
  }

  dimension_group: start {
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
    sql: ${TABLE}.start_time ;;
  }

  dimension: target_cloud {
    type: string
    sql: ${TABLE}.target_cloud ;;
  }

  dimension: target_region {
    type: string
    sql: ${TABLE}.target_region ;;
  }

  dimension: is_prior_month_mtd {
    type: yesno
    sql:  DATE_TRUNC('month', ${start_raw}) = DATE_TRUNC('month', (current_timestamp - interval '1 month'))
      and ${start_raw} <= current_timestamp - interval '1 month'  ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }


  measure: bytes_transferred {
    type: sum
    sql: ${TABLE}.bytes_transferred ;;
  }

  measure: current_mtd_bytes_transferred {
    type: sum
    sql:  ${TABLE}.bytes_transferred / power(1024,4) ;;
    filters: {field: start_time value: "this month"}
    value_format_name: decimal_2
  }

  measure: prior_mtd_bytes_transferred {
    type: sum
    sql:  ${TABLE}.bytes_transferred / power(1024,4);;
    filters: {field: is_prior_month_mtd value: "yes"}
    value_format_name: decimal_2

  }

  measure: current_mtd_cost {
    type: sum
    sql:  ( ${TABLE}.bytes_transferred / power(1024,4)) * 0.02 ;;
    filters: {field: start_time value: "this month"}
    value_format_name: usd_0
  }

  measure: prior_mtd_cost {
    type: sum
    sql: ( ${TABLE}.bytes_transferred / power(1024,4)) * 0.02 ;;
    filters: {field: is_prior_month_mtd value: "yes"}
    value_format_name: usd_0
    }
}
