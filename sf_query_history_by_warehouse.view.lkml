view: sf_query_history_by_warehouse {
  sql_table_name: LOOKER.QUERY_HISTORY_BY_WAREHOUSE ;;

  dimension_group: query_date {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}."DATE_ID" ;;
  }

  measure: exec_time_sec_50_pctile{
    type: max
    sql: ${TABLE}."P50" ;;
    value_format_name: decimal_2
  }

  measure: exec_time_sec_90_pctile {
    type: max
    sql: ${TABLE}."P90" ;;
    value_format_name: decimal_2
  }

  measure: exec_time_sec_99_pctile {
    type: max
    sql: ${TABLE}."P99" ;;
    value_format_name: decimal_2
  }

  dimension: warehouse_id {
    type: string
    sql: ${TABLE}."WAREHOUSE_ID" ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
