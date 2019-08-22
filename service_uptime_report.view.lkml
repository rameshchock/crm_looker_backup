view: service_uptime_report {
  sql_table_name: process_metrics.service_uptime_report;;

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

  measure: total_time_in_secs {
    type: sum
    sql: ${TABLE}.total_time ;;
    value_format_name: decimal_0
  }

  measure: up_time_in_secs {
    type: sum
    sql: ${TABLE}.up_time ;;
    value_format_name: decimal_0
  }

  measure: uptime_percentage {
    type: average
    sql: case when (${TABLE}.up_time>0 or ${TABLE}.down_time>0 )
    then (${TABLE}.up_time*1.0000)/(${TABLE}.total_time*1.0000)
    else 0 end;;
    value_format_name:percent_2
  }

  measure: count {
    type: count
    drill_fields: [service_name]
  }
}
