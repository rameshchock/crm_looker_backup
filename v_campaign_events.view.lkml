view: v_campaign_events {
  sql_table_name: campaign_aggregates.v_campaign_events ;;

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

  dimension: template_id {
    type: string
    sql: ${TABLE}.template_id ;;
  }


  measure: message_unsubscribed {
    type: sum
    sql: ${TABLE}.message_unsubscribed ;;
  }

  measure: messages_bounced {
    type: sum
    sql: ${TABLE}.messages_bounced ;;
  }

  measure: messages_clicked {
    type: sum
    sql: ${TABLE}.messages_clicked ;;
  }

  measure: messages_delivered {
    type: sum
    sql: ${TABLE}.messages_delivered ;;
  }

  measure: messages_opened {
    type: sum
    sql: ${TABLE}.messages_opened ;;
  }

  measure: messages_sent {
    type: sum
    sql: ${TABLE}.messages_sent ;;
  }

  measure: messages_skipped {
    type: sum
    sql: ${TABLE}.messages_skipped ;;
  }


  measure: count {
    type: count
    drill_fields: []
  }
}
