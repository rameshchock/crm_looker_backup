view: site_id_by_min {
  sql_table_name: site_event_aggregates.site_id_by_min ;;

  dimension_group: etl {
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
    sql: ${TABLE}.etl_time ;;
  }

  dimension: events_count {
    type: number
    sql: ${TABLE}.events_count ;;
  }

  dimension_group: events {
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
    sql: ${TABLE}.events_time ;;
  }

  dimension: file_name {
    type: string
    sql: ${TABLE}.file_name ;;
  }

  dimension: site_id {
    type: string
    sql: ${TABLE}.site_id ;;
  }

  dimension: unique_events {
    type: number
    sql: ${TABLE}.unique_events ;;
  }

  measure: count {
    type: count
    drill_fields: [file_name]
  }
}
