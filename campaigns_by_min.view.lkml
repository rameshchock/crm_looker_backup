view: campaigns_by_min {
  sql_table_name: campaign_aggregates.campaigns_by_min ;;

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

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension: events_count {
    type: number
    sql: ${TABLE}.events_count ;;
  }

  dimension: file_name {
    type: string
    sql: ${TABLE}.file_name ;;
  }

  dimension_group: message {
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
    sql: ${TABLE}.message_time ;;
  }

  dimension: recurrence_index {
    type: string
    sql: ${TABLE}.recurrence_index ;;
  }

  dimension: site_id {
    type: string
    sql: ${TABLE}.site_id ;;
  }

  dimension: template_id {
    type: string
    sql: ${TABLE}.template_id ;;
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
