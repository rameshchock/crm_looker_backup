view: sf_warehouse_utilization {
  sql_table_name: LOOKER.WAREHOUSE_UTILIZATION ;;

  dimension: date_id {
    type: date
    sql: ${TABLE}."DATE_ID" ;;
  }

  measure: max_available_slots {
    type: sum
    sql: ${TABLE}."TOTAL_SLOTS" ;;
  }

  measure: active_slots {
    type: sum
    sql: ${TABLE}."TOTAL_SLOTS" - ${TABLE}."TURNED_OFF_SLOTS";;
  }

  measure: turned_off_slots {
    type: sum
    sql: ${TABLE}."TURNED_OFF_SLOTS" ;;
  }

  measure: underutilization_pct {
    type: average
    sql: ${TABLE}."UNDERUTILIZED_PCT" ;;
    value_format_name: percent_1
  }

  measure: underutilized_slots {
    type: sum
    sql: ${TABLE}."UNDERUTILIZED_SLOTS" ;;
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
