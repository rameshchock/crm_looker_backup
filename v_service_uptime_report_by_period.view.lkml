view: v_service_uptime_report_by_period {
  sql_table_name: process_metrics.v_service_uptime_report_by_period ;;

  dimension: period {
    type: string
    sql: ${TABLE}.period ;;
  }

  dimension: service_name {
    type: string
    sql: ${TABLE}.service_name ;;
  }

  dimension_group: report {
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
    sql: ${TABLE}.report_date ;;
  }

  measure: down_time_in_secs {
    type: sum
    sql: ${TABLE}.down_time ;;
    value_format_name: decimal_0
  }

  measure: up_time_in_secs {
    type: sum
    sql: ${TABLE}.up_time ;;
    value_format_name: decimal_0
  }

  measure: uptime_percentage {
    type: average
    sql: ${TABLE}.up_perc;;
    value_format_name:percent_2
  }

  measure: count {
    type: count
    drill_fields: [service_name]
  }
}
