view: aggr_site_id_daily {
  sql_table_name: site_event_aggregates.aggr_site_id_daily ;;

  dimension: events_count {
    type: number
    sql: ${TABLE}.events_count ;;
  }

  dimension_group: events {
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
    sql: ${TABLE}.events_date ;;
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
