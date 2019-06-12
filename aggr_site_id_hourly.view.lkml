view: aggr_site_id_hourly {
  sql_table_name: site_event_aggregates.aggr_site_id_hourly ;;

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

  dimension: site_id {
    type: string
    sql: ${TABLE}.site_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
