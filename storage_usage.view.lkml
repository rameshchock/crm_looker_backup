view: storage_usage {
  sql_table_name: ACCOUNT_USAGE.STORAGE_USAGE ;;

  dimension: failsafe_bytes {
    type: string
    sql: ${TABLE}."FAILSAFE_BYTES" ;;
  }

  dimension: stage_bytes {
    type: string
    sql: ${TABLE}."STAGE_BYTES" ;;
  }

  dimension: storage_bytes {
    type: string
    sql: ${TABLE}."STORAGE_BYTES" ;;
  }

  dimension_group: usage {
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
    sql: ${TABLE}."USAGE_DATE" ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
