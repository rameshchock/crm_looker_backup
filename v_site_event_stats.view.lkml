view: v_site_event_stats {
  sql_table_name: process_metrics.v_site_event_stats ;;

  dimension: actual_elapsed_time {
    type: string
    sql: ${TABLE}.actual_elapsed_time ;;
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension_group: process_end {
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
    sql: ${TABLE}.process_end_time ;;
  }

  dimension_group: process_start {
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
    sql: ${TABLE}.process_start_time ;;
  }

  dimension: process_uid {
    type: string
    sql: ${TABLE}.process_uid ;;
  }

  dimension: site_id {
    type: string
    sql: ${TABLE}.site_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: elapsed_time_secs {
    type: sum
    sql: ${TABLE}.elapsed_time ;;
  }
}
