view: v_events_kpi_report_by_period {
  sql_table_name: site_event_aggregates.v_events_kpi_report_by_period ;;

  measure: email_recs {
    type: max
    sql: ${TABLE}.email_recs ;;
  }

  measure: emails_sent {
    type: max
    sql: ${TABLE}.emails_sent ;;
  }

  measure: peak_emails_per_hr {
    type: max
    sql: ${TABLE}.peak_emails_per_hr ;;
  }

  measure: peak_events_per_sec {
    type: max
    sql: ${TABLE}.peak_events_per_sec ;;
  }

  measure: peak_recs_per_sec {
    type: max
    sql: ${TABLE}.peak_recs_per_sec ;;
  }

  dimension: period_name {
    type: string
    sql: ${TABLE}.period_name ;;
  }

  measure: sms_sent {
    type: max
    sql: ${TABLE}.sms_sent ;;
  }

  measure: total_events {
    type: max
    sql: ${TABLE}.total_events ;;
  }

  measure: total_recs {
    type: max
    sql: ${TABLE}.total_recs ;;
  }

  measure: web_recs {
    type: max
    sql: ${TABLE}.web_recs ;;
  }

  measure: count {
    type: count
    drill_fields: [period_name]
  }
}
