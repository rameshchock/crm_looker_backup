view: site_id_part {
  sql_table_name: site_event_aggregates.site_id_part ;;

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

  dimension: events_cnt {
    type: number
    sql: ${TABLE}.events_cnt ;;
  }

  dimension: file_name {
    type: string
    sql: ${TABLE}.file_name ;;
  }

  dimension_group: max_event {
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
    sql: ${TABLE}.max_event_time ;;
  }

  dimension_group: min_event {
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
    sql: ${TABLE}.min_event_time ;;
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
