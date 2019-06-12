view: warehouse_metering_history {
  sql_table_name: ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY ;;

  dimension: credits_used {
    type: string
    sql: ${TABLE}."CREDITS_USED" ;;
  }

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
    sql: ${TABLE}."END_TIME" ;;
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
    sql: ${TABLE}."START_TIME" ;;
  }

  dimension: warehouse_id {
    type: string
    sql: ${TABLE}."WAREHOUSE_ID" ;;
  }

  dimension: warehouse_name {
    type: string
    sql: ${TABLE}."WAREHOUSE_NAME" ;;
  }

  measure: count {
    type: count
    drill_fields: [warehouse_name]
  }
}
