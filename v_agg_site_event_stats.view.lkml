view: v_agg_site_event_stats {
  sql_table_name: process_metrics.v_agg_site_event_stats ;;

  dimension_group: event_date {
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
    sql: ${TABLE}.event_date ;;
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension: site_id {
    type: string
    sql: ${TABLE}.site_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: average_time {
    type: average
    sql: ${TABLE}.average_time ;;
    value_format_name: decimal_0
  }

  measure: event_count {
    type: sum
    sql: ${TABLE}.event_count ;;
  }
}
